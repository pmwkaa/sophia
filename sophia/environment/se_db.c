
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
	scheme->id              = sr_seq(&e->seq, SR_DSNNEXT);
	scheme->sync            = 1;
	scheme->mmap            = 0;
	scheme->compression     = 0;
	scheme->compression_key = 0;
	scheme->compression_if  = &ss_nonefilter;
	scheme->fmt             = SF_KV;
	scheme->fmt_storage     = SF_SRAW;
	sf_updateinit(&scheme->fmt_update);
	scheme->compression_sz = ss_strdup(&e->a, scheme->compression_if->name);
	if (ssunlikely(scheme->compression_sz == NULL))
		goto e1;
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
			         "and comppression_key=1");
			return -1;
		}
		s->fmt_storage = SF_SKEYVALUE;
	}
	/* compression */
	if (strcmp(s->compression_sz, "none") == 0) {
		s->compression_if = &ss_nonefilter;
	} else
	if (strcmp(s->compression_sz, "zstd") == 0) {
		s->compression_if = &ss_zstdfilter;
	} else
	if (strcmp(s->compression_sz, "lz4") == 0) {
		s->compression_if = &ss_lz4filter;
	} else {
		sr_error(&e->error, "unknown compression type '%s'",
		         s->compression_sz);
		return -1;
	}
	s->compression = s->compression_if != &ss_nonefilter;
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

	db->r.scheme = &s->scheme;
	db->r.fmt = s->fmt;
	db->r.fmt_storage = s->fmt_storage;
	db->r.fmt_update  = &s->fmt_update;
	db->r.compression = s->compression_if;
	return 0;
}

static int
se_dbasync_set(so *o, so *v)
{
	se_cast(o, so*, SEDBASYNC);
	sedb *db = se_cast(o->parent, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbwrite(db, key, 1, 0);
}

static int
se_dbasync_update(so *o, so *v)
{
	se_cast(o, so*, SEDBASYNC);
	sedb *db = se_cast(o->parent, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbwrite(db, key, 1, SVUPDATE);
}

static int
se_dbasync_del(so *o, so *v)
{
	se_cast(o, so*, SEDBASYNC);
	sedb *db = se_cast(o->parent, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbwrite(db, key, 1, SVDELETE);
}

static void*
se_dbasync_get(so *o, so *v)
{
	se_cast(o, so*, SEDBASYNC);
	sedb *db = se_cast(o->parent, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbget(db, key, 1, 0, 1);
}

static void*
se_dbasync_cursor(so *o, so *v)
{
	se_cast(o, so*, SEDBASYNC);
	sedb *db = se_cast(o->parent, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_cursornew(db, key, 0, 1);
}

static void*
se_dbasync_object(so *o)
{
	se_cast(o, so*, SEDBASYNC);
	sedb *db = se_cast(o->parent, sedb*, SEDB);
	se *e = se_of(&db->o);
	return se_vnew(e, &db->o, NULL);
}

static soif sedbasyncif =
{
	.open         = NULL,
	.destroy      = NULL,
	.error        = NULL,
	.object       = se_dbasync_object,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = se_dbasync_set,
	.update       = se_dbasync_update,
	.del          = se_dbasync_del,
	.get          = se_dbasync_get,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = se_dbasync_cursor,
};

static void*
se_dbasync(so *o)
{
	return &se_cast(o, sedb*, SEDB)->async;
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
	sx_indexset(&db->coindex, db->scheme.id, db->r.scheme);
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
	rc = so_listdestroy(&db->cursor);
	if (ssunlikely(rc == -1))
		rcret = -1;
	sx_indexfree(&db->coindex, &e->xm);
	rc = si_close(&db->index);
	if (ssunlikely(rc == -1))
		rcret = -1;
	si_schemefree(&db->scheme, &db->r);
	sd_cfree(&db->dc, &db->r);
	se_statusfree(&db->status);
	ss_spinlockfree(&db->reflock);
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

static int
se_dbset(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbwrite(db, key, 0, 0);
}

static int
se_dbupdate(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbwrite(db, key, 0, SVUPDATE);
}

static int
se_dbdel(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbwrite(db, key, 0, SVDELETE);
}

static void*
se_dbget(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_txdbget(db, key, 0, 0, 1);
}

static void*
se_dbcursor(so *o, so *v)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	sev *key = se_cast(v, sev*, SEV);
	return se_cursornew(db, key, 0, 0);
}

static void*
se_dbobject(so *o)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	se *e = se_of(&db->o);
	return se_vnew(e, &db->o, NULL);
}

static void*
se_dbget_string(so *o, char *path, int *size)
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
se_dbget_int(so *o, char *path)
{
	sedb *db = se_cast(o, sedb*, SEDB);
	if (strcmp(path, "id") == 0)
		return db->scheme.id;
	return 0;
}

static soif sedbif =
{
	.open         = se_dbopen,
	.destroy      = se_dbdestroy,
	.error        = NULL,
	.object       = se_dbobject,
	.asynchronous = se_dbasync,
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
	.cursor       = se_dbcursor,
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
	so_init(&o->async, &se_o[SEDBASYNC], &sedbasyncif, &o->o, &e->o);
	so_listinit(&o->cursor);
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
		ss_free(&e->a_db, o);
		si_schemefree(&o->scheme, &o->r);
		return NULL;
	}
	sx_indexinit(&o->coindex, &o->r, o);
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

svv *se_dbv(sedb *db, sev *o, int search)
{
	se *e = se_of(&db->o);
	svv *v;
	/* reuse object */
	if (o->v.v) {
		v = sv_vdup(db->r.a, &o->v);
		goto ret;
	}
	/* create object from raw data */
	if (o->raw) {
		v = sv_vbuildraw(db->r.a, o->raw, o->rawsize);
		goto ret;
	}
	/* create object using current format, supplied
	 * key-chain and value */
	if (ssunlikely(o->keyc != db->scheme.scheme.count)) {
		sr_error(&e->error, "%s", "bad object key");
		return NULL;
	}
	/* switch to key-value format to avoid value
	 * copy during search operations */
	sr *runtime = &db->r;
	sr  runtime_search;
	if (search && db->r.fmt == SF_DOCUMENT) {
		runtime_search = db->r;
		runtime_search.fmt = SF_KV;
		runtime = &runtime_search;
	}
	v = sv_vbuild(runtime, o->keyv, o->keyc,
	              o->value,
	              o->valuesize);
ret:
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	return v;
}
