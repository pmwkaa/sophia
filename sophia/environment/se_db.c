
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libso.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libse.h>

static int
se_dbscheme_init(sedb *db, char *name)
{
	se *e = se_of(&db->o);
	/* prepare index scheme */
	sischeme *scheme = &db->scheme;
	scheme->name = ss_strdup(&e->a, name);
	if (ssunlikely(scheme->name == NULL))
		goto e0;
	scheme->id                    = sr_seq(&e->seq, SR_DSNNEXT);
	scheme->sync                  = 2;
	scheme->mmap                  = 0;
	scheme->in_memory             = 0;
	scheme->compression_key       = 0;
	scheme->compression           = 0;
	scheme->compression_if        = &ss_nonefilter;
	scheme->compression_branch    = 0;
	scheme->compression_branch_if = &ss_nonefilter;
	scheme->fmt                   = SF_KV;
	scheme->fmt_storage           = SF_SRAW;
	scheme->path_fail_on_exists   = 0;
	scheme->path_fail_on_drop     = 1;
	scheme->buf_gc_wm             = 1024 * 1024;
	scheme->compression_sz =
		ss_strdup(&e->a, scheme->compression_if->name);
	if (ssunlikely(scheme->compression_sz == NULL))
		goto e1;
	scheme->compression_branch_sz =
		ss_strdup(&e->a, scheme->compression_branch_if->name);
	if (ssunlikely(scheme->compression_branch_sz == NULL))
		goto e1;
	sf_updateinit(&scheme->fmt_update);
	scheme->fmt_sz = ss_strdup(&e->a, "kv");
	if (ssunlikely(scheme->fmt_sz == NULL))
		goto e1;
	/* init single key part as string */
	int rc;
	sr_schemeinit(&scheme->scheme);
	srkey *part = sr_schemeadd(&scheme->scheme, &e->a);
	if (ssunlikely(part == NULL))
		goto e1;
	rc = sr_keysetname(part, &e->a, "key");
	if (ssunlikely(rc == -1))
		goto e1;
	rc = sr_keyset(part, &e->a, "string");
	if (ssunlikely(rc == -1))
		goto e1;
	return 0;
e1:
	si_schemefree(&db->scheme, &db->r);
e0:
	sr_oom(&e->error);
	return -1;
}

static int
se_dbscheme_set(sedb *db)
{
	se *e = se_of(&db->o);
	sischeme *s = &db->scheme;
	/* format */
	if (strcmp(s->fmt_sz, "kv") == 0) {
		s->fmt = SF_KV;
	} else
	if (strcmp(s->fmt_sz, "document") == 0) {
		s->fmt = SF_DOCUMENT;
	} else {
		sr_error(&e->error, "unknown format type '%s'", s->fmt_sz);
		return -1;
	}
	/* compression_key */
	if (s->compression_key) {
		if (s->fmt == SF_DOCUMENT) {
			sr_error(&e->error, "%s", "incompatible options: format=document "
			         "and compression_key=1");
			return -1;
		}
		s->fmt_storage = SF_SKEYVALUE;
	}
	/* compression */
	s->compression_if = ss_filterof(s->compression_sz);
	if (ssunlikely(s->compression_if == NULL)) {
		sr_error(&e->error, "unknown compression type '%s'",
		         s->compression_sz);
		return -1;
	}
	s->compression = s->compression_if != &ss_nonefilter;
	/* compression branch */
	s->compression_branch_if = ss_filterof(s->compression_branch_sz);
	if (ssunlikely(s->compression_branch_if == NULL)) {
		sr_error(&e->error, "unknown compression type '%s'",
		         s->compression_branch_sz);
		return -1;
	}
	s->compression_branch = s->compression_branch_if != &ss_nonefilter;
	/* path */
	if (s->path == NULL) {
		char path[1024];
		snprintf(path, sizeof(path), "%s/%s", e->meta.path, s->name);
		s->path = ss_strdup(&e->a, path);
		if (ssunlikely(s->path == NULL))
			return sr_oom(&e->error);
	}
	/* backup path */
	s->path_backup = e->meta.backup_path;
	if (e->meta.backup_path) {
		s->path_backup = ss_strdup(&e->a, e->meta.backup_path);
		if (ssunlikely(s->path_backup == NULL))
			return sr_oom(&e->error);
	}
	/* compaction */
	s->node_size          = e->meta.node_size;
	s->node_page_size     = e->meta.page_size;
	s->node_page_checksum = e->meta.page_checksum;
	s->node_compact_load  = e->meta.node_preload;

	db->r.scheme = &s->scheme;
	db->r.fmt = s->fmt;
	db->r.fmt_storage = s->fmt_storage;
	db->r.fmt_update  = &s->fmt_update;
	return 0;
}

static int
se_dbopen(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	int status = se_status(&db->status);
	if (status == SE_RECOVER)
		goto online;
	if (status != SE_OFFLINE)
		return -1;
	int rc = se_dbscheme_set(db);
	if (ssunlikely(rc == -1))
		return -1;
	sx_indexset(&db->coindex, db->scheme.id);
	rc = se_recoverbegin(db);
	if (ssunlikely(rc == -1))
		return -1;
	if (se_status(&e->status) == SE_RECOVER)
		return 0;
online:
	se_recoverend(db);
	rc = se_scheduler_add(&e->sched, db);
	if (ssunlikely(rc == -1))
		return -1;
	db->scheduled = 1;
	return 0;
}

static int
se_dbdestroy(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	int rcret = 0;
	int rc;
	int status = se_status(&e->status);
	if (status == SE_SHUTDOWN)
		goto shutdown;

	uint32_t ref;
	status = se_status(&db->status);
	switch (status) {
	case SE_MALFUNCTION:
	case SE_ONLINE:
	case SE_RECOVER:
		ref = se_dbunref(db, 0);
		if (ref > 0)
			return 0;
		/* set last visible transaction id */
		db->txn_max = sx_max(&e->xm);
		if (db->scheduled) {
			rc = se_scheduler_del(&e->sched, o);
			if (ssunlikely(rc == -1))
				return -1;
		}
		so_listdel(&e->db, &db->o);
		ss_spinlock(&e->dblock);
		so_listadd(&e->db_shutdown, &db->o);
		ss_spinunlock(&e->dblock);
		se_statusset(&db->status, SE_SHUTDOWN);
		return 0;
	case SE_SHUTDOWN:
		/* this intended to be called from a
		 * background gc task */
		assert(se_dbrefof(db, 0) == 0);
		ref = se_dbrefof(db, 1);
		if (ref > 0)
			return 0;
		goto shutdown;
	case SE_OFFLINE:
		so_listdel(&e->db, &db->o);
		goto shutdown;
	default: assert(0);
	}

shutdown:;
	sx_indexfree(&db->coindex, &e->xm);
	rc = si_close(&db->index);
	if (ssunlikely(rc == -1))
		rcret = -1;
	si_schemefree(&db->scheme, &db->r);
	sd_cfree(&db->dc, &db->r);
	se_statusfree(&db->status);
	ss_spinlockfree(&db->reflock);
	se_mark_destroyed(&db->o);
	ss_free(&e->a_db, db);
	return rcret;
}

static int
se_dbdrop(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	int status = se_status(&db->status);
	if (ssunlikely(! se_statusactive_is(status)))
		return -1;
	if (ssunlikely(db->dropped))
		return 0;
	int rc = si_dropmark(&db->index);
	if (ssunlikely(rc == -1))
		return -1;
	db->dropped = 1;
	return 0;
}

void*
se_dbread(sedb *db, sev *o, sx *x, int x_search,
          sicache *cache, ssorder order)
{
	se *e = se_of(&db->o);
	/* validate request */
	if (ssunlikely(o->o.parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return NULL;
	}
	if (ssunlikely(! se_online(&db->status)))
		goto e0;
	int cache_only  = o->cache_only;
	int async       = o->async;
	void *async_arg = o->async_arg;

	uint64_t start  = ss_utime();
	/* prepare search key */
	svv *v;
	int rc = se_dbv(db, o, 1, &v);
	if (ssunlikely(rc == -1))
		goto e0;
	if (v) {
		v->flags = SVGET;
	}
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	svv *vprf;
	rc = se_dbvprefix(db, o, &vprf);
	if (ssunlikely(rc == -1))
		goto e1;
	sv vprefix;
	sv_init(&vprefix, &sv_vif, vprf, NULL);
	if (vprf && v == NULL) {
		v = sv_vdup(&db->r, &vprefix);
		sv_init(&vp, &sv_vif, v, NULL);
	}
	sv vup;
	memset(&vup, 0, sizeof(vup));
	so_destroy(&o->o);
	o = NULL;

	/* concurrent */
	if (x_search) {
		/* note: prefix is ignored during concurrent
		 * index search */
		assert(v != NULL);
		int rc = sx_get(x, &db->coindex, &vp, &vup);
		if (ssunlikely(rc == -1 || rc == 2 /* delete */))
			goto e2;
		if (rc == 1 && !sv_is(&vup, SVUPDATE)) {
			so *ret = se_vnew(e, &db->o, &vup, async);
			if (ssunlikely(ret == NULL))
				sv_vfree(&db->r, vup.v);
			if (async) {
				sev *match = (sev*)ret;
				match->async_operation = SE_REQREAD;
				match->async_status    = 1;
				match->async_arg       = async_arg;
				match->async_seq       = 0;
				match->cache_only      = cache_only;
			}
			if (vprf)
				sv_vfree(&db->r, vprf);
			sv_vfree(&db->r, v);
			return ret;
		}
	} else {
		sx_getstmt(&e->xm, &db->coindex);
	}

	/* prepare read cache */
	int cachegc = 0;
	if (cache == NULL) {
		cachegc = 1;
		cache = si_cachepool_pop(&e->cachepool);
		if (ssunlikely(cache == NULL)) {
			sr_oom(&e->error);
			goto e2;
		}
	}

	/* prepare request */
	sereq q;
	se_reqinit(e, &q, SE_REQREAD, &db->o, &db->o);
	q.start = start;
	sereqarg *arg = &q.arg;
	arg->v          = vp;
	arg->vup        = vup;
	arg->vprefix    = vprefix;
	arg->cache      = cache;
	arg->cachegc    = cachegc;
	arg->order      = order;
	arg->arg        = async_arg;
	arg->cache_only = cache_only;
	if (x) {
		arg->vlsn = x->vlsn;
		arg->vlsn_generate = 0;
	} else {
		arg->vlsn = 0;
		arg->vlsn_generate = 1;
	}
	if (sf_updatehas(&db->scheme.fmt_update)) {
		arg->update = 1;
		if (arg->order == SS_EQ) {
			arg->order = SS_GTE;
			arg->update_eq = 1;
		}
	}

	/* asynchronous */
	if (async) {
		o = (sev*)se_reqresult(&q, 1);
		if (ssunlikely(o == NULL)) {
			se_reqend(&q);
			return NULL;
		}
		sereq *req = se_reqnew(e, &q, 1);
		if (ssunlikely(req == NULL)) {
			so_destroy(&o->o);
			se_reqend(&q);
			return NULL;
		}
		return o;
	}

	/* synchronous */
	rc = se_execute(&q);
	if (rc == 1)
		o = (sev*)se_reqresult(&q, async);
	se_reqend(&q);
	return o;
e2: if (vprf)
		sv_vfree(&db->r, vprf);
e1: if (v)
		sv_vfree(&db->r, v);
e0: if (o)
		so_destroy(&o->o);
	return NULL;
}

static inline int
se_dbwrite(sedb *db, sev *o, uint8_t flags)
{
	se *e = se_of(&db->o);
	/* validate req */
	if (ssunlikely(o->o.parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return -1;
	}
	if (ssunlikely(! se_online(&db->status)))
		goto error;
	if (flags == SVUPDATE && !sf_updatehas(&db->scheme.fmt_update))
		flags = 0;

	/* prepare object */
	svv *v;
	int rc = se_dbv(db, o, 0, &v);
	if (ssunlikely(rc == -1))
		goto error;
	so_destroy(&o->o);
	v->flags = flags;

	/* ensure quota */
	ss_quota(&e->quota, SS_QADD, sv_vsize(v));

	/* single-statement transaction */
	sx t;
	sx_begin(&e->xm, &t, SXRW, 0);
	rc = sx_set(&t, &db->coindex, v);
	if (ssunlikely(rc == -1)) {
		ss_quota(&e->quota, SS_QREMOVE, sv_vsize(v));
		return -1;
	}
	sxstate s = sx_prepare(&t, NULL, NULL);
	switch (s) {
	case SXLOCK: sx_rollback(&t);
		return 2;
	case SXROLLBACK:
		return 1;
	default: break;
	}
	sx_commit(&t);

	/* execute req */
	sereq q;
	se_reqinit(e, &q, SE_REQWRITE, &db->o, &db->o);
	sereqarg *arg = &q.arg;
	arg->vlsn_generate = 1;
	arg->lsn = 0;
	arg->recover = 0;
	arg->log = &t.log;
	se_execute(&q);
	if (ssunlikely(q.rc == -1))
		ss_quota(&e->quota, SS_QREMOVE, sv_vsize(v));
	se_reqend(&q);

	sx_gc(&t);
	return q.rc;
error:
	so_destroy(&o->o);
	return -1;
}

static int
se_dbset(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	se *e = se_of(&db->o);
	uint64_t start = ss_utime();
	int rc = se_dbwrite(db, key, 0);
	sr_statset(&e->stat, start);
	return rc;
}

static int
se_dbupdate(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	se *e = se_of(&db->o);
	uint64_t start = ss_utime();
	int rc = se_dbwrite(db, key, SVUPDATE);
	sr_statupdate(&e->stat, start);
	return rc;
}

static int
se_dbdel(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	se *e = se_of(&db->o);
	uint64_t start = ss_utime();
	int rc = se_dbwrite(db, key, SVDELETE);
	sr_statdelete(&e->stat, start);
	return rc;
}

static void*
se_dbget(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_dbread(db, key, NULL, 0, NULL, key->order);
}

static void*
se_dbobject(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	return se_vnew(e, &db->o, NULL, 0);
}

static void*
se_dbget_string(so *o, const char *path, int *size)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	if (strcmp(path, "name") == 0) {
		int namesize = strlen(db->scheme.name) + 1;
		if (size)
			*size = namesize;
		char *name = malloc(namesize);
		if (name == NULL)
			return NULL;
		memcpy(name, db->scheme.name, namesize);
		return name;
	}
	return NULL;
}

static int64_t
se_dbget_int(so *o, const char *path)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	if (strcmp(path, "id") == 0)
		return db->scheme.id;
	else
	if (strcmp(path, "key-count") == 0)
		return db->scheme.scheme.count;
	return -1;
}

static soif sedbif =
{
	.open         = se_dbopen,
	.destroy      = se_dbdestroy,
	.error        = NULL,
	.object       = se_dbobject,
	.poll         = NULL,
	.drop         = se_dbdrop,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = se_dbget_string,
	.getint       = se_dbget_int,
	.set          = se_dbset,
	.update       = se_dbupdate,
	.del          = se_dbdel,
	.get          = se_dbget,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_dbnew(se *e, char *name)
{
	sedb *o = ss_malloc(&e->a_db, sizeof(sedb));
	if (ssunlikely(o == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memset(o, 0, sizeof(*o));
	so_init(&o->o, &se_o[SEDB], &sedbif, &e->o, &e->o);
	se_statusinit(&o->status);
	se_statusset(&o->status, SE_OFFLINE);
	o->r         = e->r;
	o->r.scheme  = &o->scheme.scheme;
	o->created   = 0;
	o->scheduled = 0;
	o->dropped   = 0;
	memset(&o->rtp, 0, sizeof(o->rtp));
	int rc = se_dbscheme_init(o, name);
	if (ssunlikely(rc == -1)) {
		ss_free(&e->a_db, o);
		return NULL;
	}
	rc = si_init(&o->index, &o->r);
	if (ssunlikely(rc == -1)) {
		si_schemefree(&o->scheme, &o->r);
		ss_free(&e->a_db, o);
		return NULL;
	}
	sx_indexinit(&o->coindex, &e->xm, &o->r, o);
	ss_spinlockinit(&o->reflock);
	o->ref_be = 0;
	o->ref = 0;
	o->txn_min = sx_min(&e->xm);
	o->txn_max = o->txn_min;
	sd_cinit(&o->dc);
	return &o->o;
}

so *se_dbmatch(se *e, char *name)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		if (strcmp(db->scheme.name, name) == 0)
			return &db->o;
	}
	return NULL;
}

so *se_dbmatch_id(se *e, uint32_t id)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		if (db->scheme.id == id)
			return &db->o;
	}
	return NULL;
}

void se_dbref(sedb *o, int be)
{
	ss_spinlock(&o->reflock);
	if (be)
		o->ref_be++;
	else
		o->ref++;
	ss_spinunlock(&o->reflock);
}

uint32_t se_dbunref(sedb *o, int be)
{
	uint32_t prev_ref = 0;
	ss_spinlock(&o->reflock);
	if (be) {
		prev_ref = o->ref_be;
		if (o->ref_be > 0)
			o->ref_be--;
	} else {
		prev_ref = o->ref;
		if (o->ref > 0)
			o->ref--;
	}
	ss_spinunlock(&o->reflock);
	return prev_ref;
}

uint32_t se_dbrefof(sedb *o, int be)
{
	uint32_t ref = 0;
	ss_spinlock(&o->reflock);
	if (be)
		ref = o->ref_be;
	else
		ref = o->ref;
	ss_spinunlock(&o->reflock);
	return ref;
}

int se_dbgarbage(sedb *o)
{
	ss_spinlock(&o->reflock);
	int v = o->ref_be == 0 && o->ref == 0;
	ss_spinunlock(&o->reflock);
	return v;
}

int se_dbvisible(sedb *db, uint32_t txn)
{
	return db->txn_min < txn && txn <= db->txn_max;
}

void se_dbbind(se *e)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		int status = se_status(&db->status);
		if (se_statusactive_is(status))
			se_dbref(db, 1);
	}
}

void se_dbunbind(se *e, uint32_t txn)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		int status = se_status(&db->status);
		if (status != SE_ONLINE)
			continue;
		if (txn > db->txn_min)
			se_dbunref(db, 1);
	}
	ss_spinlock(&e->dblock);
	ss_listforeach(&e->db_shutdown.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		if (se_dbvisible(db, txn))
			se_dbunref(db, 1);
	}
	ss_spinunlock(&e->dblock);
}

int se_dbmalfunction(sedb *o)
{
	se_statusset(&o->status, SE_MALFUNCTION);
	return -1;
}

int se_dbv(sedb *db, sev *o, int search, svv **v)
{
	se *e = se_of(&db->o);
	*v = NULL;
	/* reuse object */
	if (o->v.v) {
		if (sslikely(! o->immutable)) {
			*v = o->v.v;
			o->v.v = NULL;
			return 0;
		}
		*v = sv_vbuildraw(&db->r, sv_pointer(&o->v), sv_size(&o->v));
		goto ret;
	}
	/* create object from raw data */
	if (o->raw) {
		*v = sv_vbuildraw(&db->r, o->raw, o->rawsize);
		goto ret;
	}
	sr *runtime = &db->r;
	sr  runtime_search;
	if (search) {
		if (o->keyc == 0)
			return 0;
		/* switch to key-value format to avoid value
		 * copy during search operations */
		if (db->r.fmt == SF_DOCUMENT) {
			runtime_search = db->r;
			runtime_search.fmt = SF_KV;
			runtime = &runtime_search;
		}
	}

	/* create object using current format, supplied
	 * key-chain and value */
	if (ssunlikely(o->keyc != db->scheme.scheme.count))
		return sr_error(&e->error, "%s", "bad object key");

	*v = sv_vbuild(runtime, o->keyv, o->keyc,
	               o->value,
	               o->valuesize);
ret:
	if (ssunlikely(*v == NULL))
		return sr_oom(&e->error);
	return 0;
}

int se_dbvprefix(sedb *db, sev *o, svv **v)
{
	se *e = se_of(&db->o);
	*v = NULL;
	/* reuse prefix */
	if (o->vprefix.v) {
		*v = o->vprefix.v;
		o->vprefix.v = NULL;
		return 0;
	}
	if (o->prefix == NULL)
		return 0;
	/* validate index type */
	if (sr_schemeof(&db->scheme.scheme, 0)->type != SS_STRING)
		return sr_error(&e->error, "%s", "prefix search is only "
		                "supported for a string key");
	/* create prefix object */
	sfv fv;
	fv.key      = o->prefix;
	fv.r.size   = o->prefixsize;
	fv.r.offset = 0;
	*v = sv_vbuild(&e->r, &fv, 1, NULL, 0);
	if (ssunlikely(*v == NULL))
		return -1;
	return 0;
}
