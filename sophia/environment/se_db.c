
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
#include <libsc.h>
#include <libse.h>

static int
se_dbscheme_init(sedb *db, char *name)
{
	se *e = se_of(&db->o);
	/* prepare index scheme */
	sischeme *scheme = db->scheme;
	scheme->name = ss_strdup(&e->a, name);
	if (ssunlikely(scheme->name == NULL))
		goto error;
	scheme->id                    = sr_seq(&e->seq, SR_DSNNEXT);
	scheme->sync                  = 2;
	scheme->mmap                  = 0;
	scheme->storage               = SI_SCACHE;
	scheme->cache_mode            = 0;
	scheme->cache_sz              = NULL;
	scheme->node_size             = 64 * 1024 * 1024;
	scheme->node_compact_load     = 0;
	scheme->node_page_size        = 128 * 1024;
	scheme->node_page_checksum    = 1;
	scheme->compression_key       = 0;
	scheme->compression           = 0;
	scheme->compression_if        = &ss_nonefilter;
	scheme->compression_branch    = 0;
	scheme->compression_branch_if = &ss_nonefilter;
	scheme->temperature           = 0;
	scheme->amqf                  = 0;
	scheme->fmt                   = SF_KV;
	scheme->fmt_storage           = SF_SRAW;
	scheme->path_fail_on_exists   = 0;
	scheme->path_fail_on_drop     = 1;
	scheme->lru                   = 0;
	scheme->lru_step              = 128 * 1024;
	scheme->buf_gc_wm             = 1024 * 1024;
	scheme->storage_sz = ss_strdup(&e->a, "cache");
	if (ssunlikely(scheme->storage_sz == NULL))
		goto error;
	scheme->compression_sz =
		ss_strdup(&e->a, scheme->compression_if->name);
	if (ssunlikely(scheme->compression_sz == NULL))
		goto error;
	scheme->compression_branch_sz =
		ss_strdup(&e->a, scheme->compression_branch_if->name);
	if (ssunlikely(scheme->compression_branch_sz == NULL))
		goto error;
	sf_upsertinit(&scheme->fmt_upsert);
	scheme->fmt_sz = ss_strdup(&e->a, "kv");
	if (ssunlikely(scheme->fmt_sz == NULL))
		goto error;
	/* init single key part as string */
	int rc;
	sr_schemeinit(&scheme->scheme);
	srkey *part = sr_schemeadd(&scheme->scheme);
	rc = sr_keysetname(part, &e->a, "key");
	if (ssunlikely(rc == -1))
		goto error;
	rc = sr_keyset(part, &e->a, "string");
	if (ssunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_oom(&e->error);
	return -1;
}

static int
se_dbscheme_set(sedb *db)
{
	se *e = se_of(&db->o);
	sischeme *s = si_scheme(db->index);
	/* storage */
	if (strcmp(s->storage_sz, "cache") == 0) {
		s->storage = SI_SCACHE;
	} else
	if (strcmp(s->storage_sz, "anti-cache") == 0) {
		s->storage = SI_SANTI_CACHE;
	} else
	if (strcmp(s->storage_sz, "in-memory") == 0) {
		s->storage = SI_SIN_MEMORY;
	} else {
		sr_error(&e->error, "unknown storage type '%s'", s->storage_sz);
		return -1;
	}
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
	/* upsert and format */
	if (sf_upserthas(&s->fmt_upsert)) {
		if (s->fmt == SF_DOCUMENT) {
			sr_error(&e->error, "%s", "incompatible options: format=document "
			         "and upsert function");
			return -1;
		}
		if (s->cache_mode) {
			sr_error(&e->error, "%s", "incompatible options: cache_mode=1 "
			         "and upsert function");
			return -1;
		}
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
		snprintf(path, sizeof(path), "%s/%s", e->conf.path, s->name);
		s->path = ss_strdup(&e->a, path);
		if (ssunlikely(s->path == NULL))
			return sr_oom(&e->error);
	}
	/* backup path */
	s->path_backup = e->conf.backup_path;
	if (e->conf.backup_path) {
		s->path_backup = ss_strdup(&e->a, e->conf.backup_path);
		if (ssunlikely(s->path_backup == NULL))
			return sr_oom(&e->error);
	}
	/* cache */
	if (s->cache_sz) {
		sedb *cache = (sedb*)se_dbmatch(e, s->cache_sz);
		if (ssunlikely(cache == NULL)) {
			sr_error(&e->error, "could not find cache database '%s'",
			         s->cache_sz);
			return -1;
		}
		if (ssunlikely(cache == db)) {
			sr_error(&e->error, "bad cache database '%s'",
			         s->cache_sz);
			return -1;
		}
		if (! cache->scheme->cache_mode) {
			sr_error(&e->error, "database '%s' is not in cache mode",
			         s->cache_sz);
			return -1;
		}
		if (! sr_schemeeq(&db->scheme->scheme, &cache->scheme->scheme)) {
			sr_error(&e->error, "database and cache '%s' scheme mismatch",
			         s->cache_sz);
			return -1;
		}
		si_ref(cache->index, SI_REFBE);
		db->index->cache = cache->index;
	}

	db->r->scheme = &s->scheme;
	db->r->fmt = s->fmt;
	db->r->fmt_storage = s->fmt_storage;
	db->r->fmt_upsert = &s->fmt_upsert;
	return 0;
}

static int
se_dbopen(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	int status = sr_status(&db->index->status);
	if (status == SR_RECOVER ||
	    status == SR_DROP_PENDING)
		goto online;
	if (status != SR_OFFLINE)
		return -1;
	int rc = se_dbscheme_set(db);
	if (ssunlikely(rc == -1))
		return -1;
	sx_indexset(&db->coindex, db->scheme->id);
	rc = se_recoverbegin(db);
	if (ssunlikely(rc == -1))
		return -1;

	if (sr_status(&e->status) == SR_RECOVER)
		if (e->conf.recover != SE_RECOVER_NP)
			return 0;
online:
	se_recoverend(db);
	rc = sc_add(&e->scheduler, db->index);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static inline int
se_dbfree(sedb *db, int close)
{
	se *e = se_of(&db->o);
	int rcret = 0;
	int rc;
	sx_indexfree(&db->coindex, &e->xm);
	if (close) {
		rc = si_close(db->index);
		if (ssunlikely(rc == -1))
			rcret = -1;
	}
	so_mark_destroyed(&db->o);
	ss_free(&e->a, db);
	return rcret;
}

static inline void
se_dbunref(sedb *db)
{
	se *e = se_of(&db->o);
	/* do nothing during env shutdown */
	int status = sr_status(&e->status);
	if (status == SR_SHUTDOWN)
		return;
	/* reduce reference counter */
	int ref;
	ref = si_unref(db->index, SI_REFFE);
	if (ref > 1)
		return;
	/* drop/shutdown pending:
	 *
	 * switch state and transfer job to
	 * the scheduler.
	*/
	status = sr_status(&db->index->status);
	switch (status) {
	case SR_SHUTDOWN_PENDING:
		status = SR_SHUTDOWN;
		break;
	case SR_DROP_PENDING:
		status = SR_DROP;
		break;
	default:
		return;
	}
	/* destroy database object */
	si *index = db->index;
	so_listdel(&e->db, &db->o);
	if (index->cache)
		si_unref(index->cache, SI_REFBE);
	se_dbfree(db, 0);

	/* schedule index shutdown or drop */
	sr_statusset(&index->status, status);
	sc_ctl_shutdown(&e->scheduler, index);
}

static int
se_dbdestroy(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	int status = sr_status(&e->status);
	if (status == SR_SHUTDOWN ||
	    status == SR_OFFLINE) {
		return se_dbfree(db, 1);
	}
	se_dbunref(db);
	return 0;
}

static int
se_dbclose(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	int status = sr_status(&db->index->status);
	if (ssunlikely(! sr_statusactive_is(status)))
		return -1;
	/* set last visible transaction id */
	db->txn_max = sx_max(&e->xm);
	sr_statusset(&db->index->status, SR_SHUTDOWN_PENDING);
	return 0;
}

static int
se_dbdrop(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	int status = sr_status(&db->index->status);
	if (ssunlikely(! sr_statusactive_is(status)))
		return -1;
	int rc = si_dropmark(db->index);
	if (ssunlikely(rc == -1))
		return -1;
	/* set last visible transaction id */
	db->txn_max = sx_max(&e->xm);
	sr_statusset(&db->index->status, SR_DROP_PENDING);
	return 0;
}

so *se_dbresult(se *e, scread *r)
{
	sv result;
	sv_init(&result, &sv_vif, r->result, NULL);
	r->result = NULL;

	sedocument *v =
		(sedocument*)se_document_new(e, r->db, &result);
	if (ssunlikely(v == NULL))
		return NULL;
	v->cache_only   = r->arg.cache_only;
	v->oldest_only  = r->arg.oldest_only;
	v->read_disk    = r->read_disk;
	v->read_cache   = r->read_cache;
	v->read_latency = 0;
	if (result.v) {
		v->read_latency = ss_utime() - r->start;
		sr_statget(&e->stat,
		           v->read_latency,
		           v->read_disk,
		           v->read_cache);
	}

	/* propagate current document settings to
	 * the result one */
	v->orderset = 1;
	v->order = r->arg.order;
	if (v->order == SS_GTE)
		v->order = SS_GT;
	else
	if (v->order == SS_LTE)
		v->order = SS_LT;
	/* reuse prefix document */
	v->vprefix.v = r->arg.vprefix.v;
	if (v->vprefix.v) {
		r->arg.vprefix.v = NULL;
		void *vptr = sv_vpointer(v->vprefix.v);
		v->prefix = sf_key(vptr, 0);
		v->prefixsize = sf_keysize(vptr, 0);
	}
	return &v->o;
}

void*
se_dbread(sedb *db, sedocument *o, sx *x, int x_search,
          sicache *cache, ssorder order)
{
	se *e = se_of(&db->o);
	/* validate request */
	if (ssunlikely(o->o.parent != &db->o)) {
		sr_error(&e->error, "%s", "bad document parent");
		return NULL;
	}
	if (ssunlikely(! sr_online(&db->index->status)))
		goto e0;
	int cache_only  = o->cache_only;
	int oldest_only = o->oldest_only;

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
		v = sv_vdup(db->r, &vprefix);
		sv_init(&vp, &sv_vif, v, NULL);
	}
	sv vup;
	memset(&vup, 0, sizeof(vup));
	so_destroy(&o->o);
	o = NULL;

	/* concurrent */
	if (x_search && order == SS_EQ && v) {
		/* note: prefix is ignored during concurrent
		 * index search */
		int rc = sx_get(x, &db->coindex, &vp, &vup);
		if (ssunlikely(rc == -1 || rc == 2 /* delete */))
			goto e2;
		if (rc == 1 && !sv_is(&vup, SVUPSERT)) {
			so *ret = se_document_new(e, &db->o, &vup);
			if (ssunlikely(ret == NULL))
				sv_vunref(db->r, vup.v);

			sedocument *match = (sedocument*)ret;
			match->cache_only  = cache_only;
			match->oldest_only = oldest_only;

			if (vprf)
				sv_vunref(db->r, vprf);
			sv_vunref(db->r, v);
			return ret;
		}
	} else {
		sx_get_autocommit(&e->xm, &db->coindex);
	}
	if (ssunlikely(order == SS_EQ && v == NULL))
		order = SS_GTE;

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
	scread q;
	sc_readopen(&q, db->r, &db->o, db->index);
	q.start = start;
	screadarg *arg = &q.arg;
	arg->v           = vp;
	arg->vprefix     = vprefix;
	arg->vup         = vup;
	arg->cache       = cache;
	arg->cachegc     = cachegc;
	arg->order       = order;
	arg->has         = 0;
	arg->upsert      = 0;
	arg->upsert_eq   = 0;
	arg->cache_only  = cache_only;
	arg->oldest_only = oldest_only;
	if (x) {
		arg->vlsn = x->vlsn;
		arg->vlsn_generate = 0;
	} else {
		arg->vlsn = 0;
		arg->vlsn_generate = 1;
	}
	if (sf_upserthas(&db->scheme->fmt_upsert)) {
		arg->upsert = 1;
		if (arg->order == SS_EQ) {
			arg->order = SS_GTE;
			arg->upsert_eq = 1;
		}
	}

	/* read index */
	rc = sc_read(&q, &e->scheduler);
	if (rc == 1)
		o = (sedocument*)se_dbresult(e, &q);
	sc_readclose(&q);
	return o;
e2: if (vprf)
		sv_vunref(db->r, vprf);
e1: if (v)
		sv_vunref(db->r, v);
e0: if (o)
		so_destroy(&o->o);
	return NULL;
}

static inline int
se_dbwrite(sedb *db, sedocument *o, uint8_t flags)
{
	se *e = se_of(&db->o);
	/* validate req */
	if (ssunlikely(o->o.parent != &db->o)) {
		sr_error(&e->error, "%s", "bad document parent");
		return -1;
	}
	if (ssunlikely(! sr_online(&db->index->status)))
		goto error;
	if (ssunlikely(db->scheme->cache_mode))
		goto error;
	if (flags == SVUPSERT && !sf_upserthas(&db->scheme->fmt_upsert))
		flags = 0;

	/* prepare document */
	svv *v;
	int rc = se_dbv(db, o, 0, &v);
	if (ssunlikely(rc == -1))
		goto error;
	so_destroy(&o->o);
	v->flags = flags;

	/* ensure quota */
	ss_quota(&e->quota, SS_QADD, sv_vsize(v));

	/* single-statement transaction */
	svlog log;
	sv_loginit(&log);
	sx x;
	sxstate state = sx_set_autocommit(&e->xm, &db->coindex, &x, &log, v);
	switch (state) {
	case SXLOCK: return 2;
	case SXROLLBACK: return 1;
	default: break;
	}

	/* write wal and index */
	rc = sc_write(&e->scheduler, &log, 0, 0);
	if (ssunlikely(rc == -1))
		sx_rollback(&x);

	sx_gc(&x);
	return rc;
error:
	so_destroy(&o->o);
	return -1;
}

static int
se_dbset(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	se *e = se_of(&db->o);
	uint64_t start = ss_utime();
	int rc = se_dbwrite(db, key, 0);
	sr_statset(&e->stat, start);
	return rc;
}

static int
se_dbupsert(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	se *e = se_of(&db->o);
	uint64_t start = ss_utime();
	int rc = se_dbwrite(db, key, SVUPSERT);
	sr_statupsert(&e->stat, start);
	return rc;
}

static int
se_dbdel(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
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
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	return se_dbread(db, key, NULL, 0, NULL, key->order);
}

static void*
se_dbdocument(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	return se_document_new(e, &db->o, NULL);
}

static void*
se_dbget_string(so *o, const char *path, int *size)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	if (strcmp(path, "name") == 0) {
		int namesize = strlen(db->scheme->name) + 1;
		if (size)
			*size = namesize;
		char *name = malloc(namesize);
		if (name == NULL)
			return NULL;
		memcpy(name, db->scheme->name, namesize);
		return name;
	}
	return NULL;
}

static int64_t
se_dbget_int(so *o, const char *path)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	if (strcmp(path, "id") == 0)
		return db->scheme->id;
	else
	if (strcmp(path, "key-count") == 0)
		return db->scheme->scheme.count;
	return -1;
}

static soif sedbif =
{
	.open         = se_dbopen,
	.close        = se_dbclose,
	.destroy      = se_dbdestroy,
	.free         = NULL,
	.error        = NULL,
	.document     = se_dbdocument,
	.poll         = NULL,
	.drop         = se_dbdrop,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = se_dbget_string,
	.getint       = se_dbget_int,
	.set          = se_dbset,
	.upsert       = se_dbupsert,
	.del          = se_dbdel,
	.get          = se_dbget,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_dbnew(se *e, char *name)
{
	sedb *o = ss_malloc(&e->a, sizeof(sedb));
	if (ssunlikely(o == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memset(o, 0, sizeof(*o));
	so_init(&o->o, &se_o[SEDB], &sedbif, &e->o, &e->o);
	o->index = si_init(&e->r, &o->o);
	if (ssunlikely(o->index == NULL)) {
		ss_free(&e->a, o);
		return NULL;
	}
	o->r = si_r(o->index);
	o->scheme = si_scheme(o->index);
	int rc;
	rc = se_dbscheme_init(o, name);
	if (ssunlikely(rc == -1)) {
		si_close(o->index);
		ss_free(&e->a, o);
		return NULL;
	}
	sr_statusset(&o->index->status, SR_OFFLINE);
	sx_indexinit(&o->coindex, &e->xm, o->r, &o->o, o->index);
	o->txn_min = sx_min(&e->xm);
	o->txn_max = UINT32_MAX;
	return &o->o;
}

so *se_dbmatch(se *e, char *name)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		if (strcmp(db->scheme->name, name) == 0)
			return &db->o;
	}
	return NULL;
}

so *se_dbmatch_id(se *e, uint32_t id)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		if (db->scheme->id == id)
			return &db->o;
	}
	return NULL;
}

int se_dbvisible(sedb *db, uint64_t txn)
{
	return txn > db->txn_min && txn <= db->txn_max;
}

void se_dbbind(se *e)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		int status = sr_status(&db->index->status);
		if (sr_statusactive_is(status))
			si_ref(db->index, SI_REFFE);
	}
}

void se_dbunbind(se *e, uint64_t txn)
{
	sslist *i, *n;
	ss_listforeach_safe(&e->db.list, i, n) {
		sedb *db = (sedb*)sscast(i, so, link);
		if (se_dbvisible(db, txn))
			se_dbunref(db);
	}
}

int se_dbv(sedb *db, sedocument *o, int search, svv **v)
{
	se *e = se_of(&db->o);
	*v = NULL;
	/* reuse document */
	if (o->v.v) {
		if (sslikely(! o->immutable)) {
			*v = o->v.v;
			o->v.v = NULL;
			return 0;
		}
		*v = sv_vbuildraw(db->r, sv_pointer(&o->v), sv_size(&o->v));
		goto ret;
	}
	/* create document from raw data */
	if (o->raw) {
		*v = sv_vbuildraw(db->r, o->raw, o->rawsize);
		goto ret;
	}
	sr *runtime = db->r;
	sr  runtime_search;
	if (search) {
		if (o->keyc == 0)
			return 0;
		/* switch to key-value format to avoid value
		 * copy during search operations */
		if (db->r->fmt == SF_DOCUMENT) {
			runtime_search = *db->r;
			runtime_search.fmt = SF_KV;
			runtime = &runtime_search;
		}
	}

	/* create document using current format, supplied
	 * key-chain and value */
	if (ssunlikely(o->keyc != db->scheme->scheme.count))
		return sr_error(&e->error, "%s", "bad document key");

	*v = sv_vbuild(runtime, o->keyv, o->keyc,
	               o->value,
	               o->valuesize);
ret:
	if (ssunlikely(*v == NULL))
		return sr_oom(&e->error);
	return 0;
}

int se_dbvprefix(sedb *db, sedocument *o, svv **v)
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
	if (sr_schemeof(&db->scheme->scheme, 0)->type != SS_STRING)
		return sr_error(&e->error, "%s", "prefix search is only "
		                "supported for a string key");
	/* create prefix document */
	sfv fv;
	fv.key      = o->prefix;
	fv.r.size   = o->prefixsize;
	fv.r.offset = 0;
	*v = sv_vbuild(&e->r, &fv, 1, NULL, 0);
	if (ssunlikely(*v == NULL))
		return -1;
	return 0;
}
