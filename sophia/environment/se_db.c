
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
se_dbscheme_init(sedb *db, char *name, int size)
{
	se *e = se_of(&db->o);
	/* prepare index scheme */
	sischeme *scheme = db->scheme;
	if (size == 0)
		size = strlen(name);
	scheme->name = ss_malloc(&e->a, size + 1);
	if (ssunlikely(scheme->name == NULL))
		goto error;
	memcpy(scheme->name, name, size);
	scheme->name[size] = 0;
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
	scheme->expire                = 0;
	scheme->amqf                  = 0;
	scheme->fmt_storage           = SF_RAW;
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
	sf_schemeinit(&scheme->scheme);
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
	/* set default scheme */
	int rc;
	if (s->scheme.fields_count == 0)
	{
		sffield *field = sf_fieldnew(&e->a, "key");
		if (ssunlikely(field == NULL))
			return sr_oom(&e->error);
		rc = sf_fieldoptions(field, &e->a, "string,key(0)");
		if (ssunlikely(rc == -1)) {
			sf_fieldfree(field, &e->a);
			return sr_oom(&e->error);
		}
		rc = sf_schemeadd(&s->scheme, &e->a, field);
		if (ssunlikely(rc == -1)) {
			sf_fieldfree(field, &e->a);
			return sr_oom(&e->error);
		}
		field = sf_fieldnew(&e->a, "value");
		if (ssunlikely(field == NULL))
			return sr_oom(&e->error);
		rc = sf_fieldoptions(field, &e->a, "string");
		if (ssunlikely(rc == -1)) {
			sf_fieldfree(field, &e->a);
			return sr_oom(&e->error);
		}
		rc = sf_schemeadd(&s->scheme, &e->a, field);
		if (ssunlikely(rc == -1)) {
			sf_fieldfree(field, &e->a);
			return sr_oom(&e->error);
		}
	}
	/* validate scheme and set keys */
	rc = sf_schemevalidate(&s->scheme, &e->a);
	if (ssunlikely(rc == -1)) {
		sr_error(&e->error, "incomplete scheme", s->name);
		return -1;
	}
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
	/* upsert and format */
	if (sf_upserthas(&s->fmt_upsert)) {
		if (s->cache_mode) {
			sr_error(&e->error, "%s", "incompatible options: cache_mode=1 "
			         "and upsert function");
			return -1;
		}
	}
	/* compression_key */
	if (s->compression_key) {
		s->fmt_storage = SF_SPARSE;
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
		if (! sf_schemeeq(&db->scheme->scheme, &cache->scheme->scheme)) {
			sr_error(&e->error, "database and cache '%s' scheme mismatch",
			         s->cache_sz);
			return -1;
		}
		si_ref(cache->index, SI_REFBE);
		db->index->cache = cache->index;
	}

	db->r->scheme = &s->scheme;
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

	sedocument *v = (sedocument*)se_document_new(e, r->db, &result);
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

	/* set prefix */
	if (r->arg.prefix) {
		v->prefix = r->arg.prefix;
		v->prefixcopy = r->arg.prefix;
		v->prefixsize = r->arg.prefixsize;
	}

	v->created = 1;
	v->flagset = 1;
	return &v->o;
}

void*
se_dbread(sedb *db, sedocument *o, sx *x, int x_search,
          sicache *cache)
{
	se *e = se_of(&db->o);
	uint64_t start  = ss_utime();

	/* prepare the key */
	int auto_close = !o->created;
	int rc = so_open(&o->o);
	if (ssunlikely(rc == -1))
		goto error;
	rc = se_document_validate_ro(o, &db->o);
	if (ssunlikely(rc == -1))
		goto error;
	if (ssunlikely(! sr_online(&db->index->status)))
		goto error;

	sv vup;
	sv_init(&vup, &sv_vif, NULL, NULL);

	sedocument *ret = NULL;

	/* concurrent */
	if (x_search && o->order == SS_EQ) {
		/* note: prefix is ignored during concurrent
		 * index search */
		int rc = sx_get(x, &db->coindex, &o->v, &vup);
		if (ssunlikely(rc == -1 || rc == 2 /* delete */))
			goto error;
		if (rc == 1 && !sv_is(&vup, SVUPSERT)) {
			ret = (sedocument*)se_document_new(e, &db->o, &vup);
			if (sslikely(ret)) {
				ret->cache_only  = o->cache_only;
				ret->oldest_only = o->oldest_only;
				ret->created     = 1;
				ret->orderset    = 1;
				ret->flagset     = 1;
			} else {
				sv_vunref(db->r, vup.v);
			}
			if (auto_close)
				so_destroy(&o->o);
			return ret;
		}
	} else {
		sx_get_autocommit(&e->xm, &db->coindex);
	}

	/* prepare read cache */
	int cachegc = 0;
	if (cache == NULL) {
		cachegc = 1;
		cache = si_cachepool_pop(&e->cachepool);
		if (ssunlikely(cache == NULL)) {
			if (vup.v)
				sv_vunref(db->r, vup.v);
			sr_oom(&e->error);
			goto error;
		}
	}

	sv_vref(o->v.v);

	/* prepare request */
	scread q;
	sc_readopen(&q, db->r, &db->o, db->index);
	q.start = start;
	screadarg *arg = &q.arg;
	arg->v           = o->v;
	arg->prefix      = o->prefixcopy;
	arg->prefixsize  = o->prefixsize;
	arg->vup         = vup;
	arg->cache       = cache;
	arg->cachegc     = cachegc;
	arg->order       = o->order;
	arg->has         = 0;
	arg->upsert      = 0;
	arg->upsert_eq   = 0;
	arg->cache_only  = o->cache_only;
	arg->oldest_only = o->oldest_only;
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
	if (rc == 1) {
		ret = (sedocument*)se_dbresult(e, &q);
		if (ret)
			o->prefixcopy = NULL;
	}
	sc_readclose(&q);

	if (auto_close)
		so_destroy(&o->o);
	return ret;
error:
	if (auto_close)
		so_destroy(&o->o);
	return NULL;
}

static inline int
se_dbwrite(sedb *db, sedocument *o, uint8_t flags)
{
	se *e = se_of(&db->o);

	int auto_close = !o->created;
	if (ssunlikely(! sr_online(&db->index->status)))
		goto error;
	if (ssunlikely(db->scheme->cache_mode))
		goto error;

	/* create document */
	int rc = so_open(&o->o);
	if (ssunlikely(rc == -1))
		goto error;
	rc = se_document_validate(o, &db->o, flags);
	if (ssunlikely(rc == -1))
		goto error;

	svv *v = o->v.v;
	sv_vref(v);

	/* destroy document object */
	if (auto_close) {
		/* ensure quota */
		ss_quota(&e->quota, SS_QADD, sv_vsize(v));
		so_destroy(&o->o);
	}

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
	if (auto_close)
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
	if (! sf_upserthas(&db->scheme->fmt_upsert))
		return sr_error(&e->error, "%s", "upsert callback is not set");
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
	return se_dbread(db, key, NULL, 0, NULL);
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
		return db->scheme->scheme.keys_count;
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

so *se_dbnew(se *e, char *name, int size)
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
	rc = se_dbscheme_init(o, name, size);
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
