
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
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libse.h>
#include <libso.h>

static void*
so_dbctl_get(srobj *obj, va_list args)
{
	sodbctl *ctl = (sodbctl*)obj;
	sodb *db = ctl->parent;
	src c;
	memset(&c, 0, sizeof(c));
	char *name = va_arg(args, char*);
	if (strcmp(name, "name") == 0) {
		c.name  = "name";
		c.flags = SR_CSZREF|SR_CRO;
		c.value = &db->scheme.name;
	} else
	if (strcmp(name, "id") == 0) {
		c.name  = "id";
		c.flags = SR_CU32|SR_CRO;
		c.value = &db->scheme.id;
	} else {
		return NULL;
	}
	return so_ctlreturn(&c, so_of(&db->o));
}

static void*
so_dbctl_type(srobj *o ssunused, va_list args ssunused) {
	return "database_ctl";
}

static srobjif sodbctlif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = NULL,
	.error   = NULL,
	.set     = NULL,
	.del     = NULL,
	.get     = so_dbctl_get,
	.poll    = NULL,
	.drop    = NULL,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = NULL,
	.type    = so_dbctl_type
};

static int
so_dbctl_init(sodbctl *c, void *db)
{
	sodb *o = db;
	/* init database ctl object */
	memset(c, 0, sizeof(*c));
	so *e = so_of(&o->o);
	sr_objinit(&c->o, SODBCTL, &sodbctlif, &e->o);
	c->parent    = db;
	c->created   = 0;
	c->scheduled = 0;
	c->dropped   = 0;
	return 0;
}

static int
so_dbscheme_init(sodb *db, char *name)
{
	so *e = so_of(&db->o);
	/* prepare index scheme */
	sischeme *scheme = &db->scheme;
	scheme->name = ss_strdup(&e->a, name);
	if (ssunlikely(scheme->name == NULL))
		goto e0;
	scheme->id              = sr_seq(&e->seq, SR_DSNNEXT);
	scheme->sync            = 1;
	scheme->compression     = 0;
	scheme->compression_key = 0;
	scheme->compression_if  = &ss_nonefilter;
	scheme->fmt             = SF_KV;
	scheme->fmt_storage     = SF_SRAW;
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
so_dbscheme_set(sodb *o)
{
	so *e = so_of(&o->o);
	sischeme *s = &o->scheme;

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
		snprintf(path, sizeof(path), "%s/%s", e->ctl.path, s->name);
		s->path = ss_strdup(&e->a, path);
		if (ssunlikely(s->path == NULL))
			return sr_oom(&e->error);
	}
	/* backup path */
	s->path_backup = e->ctl.backup_path;
	if (e->ctl.backup_path) {
		s->path_backup = ss_strdup(&e->a, e->ctl.backup_path);
		if (ssunlikely(s->path_backup == NULL))
			return sr_oom(&e->error);
	}
	/* compaction */
	s->node_size          = e->ctl.node_size;
	s->node_page_size     = e->ctl.page_size;
	s->node_page_checksum = e->ctl.page_checksum;

	o->r.scheme = &s->scheme;
	o->r.fmt = s->fmt;
	o->r.fmt_storage = s->fmt_storage;
	o->r.compression = s->compression_if;
	return 0;
}

static int
so_dbasync_set(srobj *obj, va_list args)
{
	sodbasync *o = (sodbasync*)obj;
	return so_txdbset(o->parent, 1, 0, args);
}

static int
so_dbasync_del(srobj *obj, va_list args)
{
	sodbasync *o = (sodbasync*)obj;
	return so_txdbset(o->parent, 1, SVDELETE, args);
}

static void*
so_dbasync_get(srobj *obj, va_list args)
{
	sodbasync *o = (sodbasync*)obj;
	return so_txdbget(o->parent, 1, 0, 1, args);
}

static void*
so_dbasync_cursor(srobj *obj, va_list args)
{
	sodbasync *o = (sodbasync*)obj;
	return so_cursornew(o->parent, 0, 1, args);
}

static void*
so_dbasync_obj(srobj *obj, va_list args ssunused)
{
	sodbasync *o = (sodbasync*)obj;
	so *e = so_of(&o->o);
	return so_vnew(e, &o->parent->o);
}

static void*
so_dbasync_type(srobj *o ssunused, va_list args ssunused) {
	return "database_async";
}

static srobjif sodbasyncif =
{
	.ctl     = NULL,
	.async   = NULL,
	.destroy = NULL,
	.error   = NULL,
	.set     = so_dbasync_set,
	.del     = so_dbasync_del,
	.get     = so_dbasync_get,
	.poll    = NULL,
	.drop    = NULL,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = so_dbasync_cursor,
	.object  = so_dbasync_obj,
	.type    = so_dbasync_type
};

static inline void
so_dbasync_init(sodbasync *a, sodb *db)
{
	so *e = so_of(&db->o);
	a->parent = db;
	sr_objinit(&a->o, SODBASYNC, &sodbasyncif, &e->o);
}

static void*
so_dbasync(srobj *obj, va_list args ssunused)
{
	sodb *o = (sodb*)obj;
	return &o->async.o;
}

static void*
so_dbctl(srobj *obj, va_list args ssunused)
{
	sodb *o = (sodb*)obj;
	return &o->ctl.o;
}

static int
so_dbopen(srobj *obj, va_list args ssunused)
{
	sodb *o = (sodb*)obj;
	so *e = so_of(&o->o);
	int status = so_status(&o->status);
	if (status == SO_RECOVER)
		goto online;
	if (status != SO_OFFLINE)
		return -1;
	int rc = so_dbscheme_set(o);
	if (ssunlikely(rc == -1))
		return -1;
	sx_indexset(&o->coindex, o->scheme.id, o->r.scheme);
	rc = so_recoverbegin(o);
	if (ssunlikely(rc == -1))
		return -1;
	if (so_status(&e->status) == SO_RECOVER)
		return 0;
online:
	so_recoverend(o);
	rc = so_scheduler_add(&e->sched, o);
	if (ssunlikely(rc == -1))
		return -1;
	o->ctl.scheduled = 1;
	return 0;
}

static int
so_dbdestroy(srobj *obj, va_list args ssunused)
{
	sodb *o = (sodb*)obj;
	so *e = so_of(&o->o);

	int rcret = 0;
	int rc;
	int status = so_status(&e->status);
	if (status == SO_SHUTDOWN)
		goto shutdown;

	uint32_t ref;
	status = so_status(&o->status);
	switch (status) {
	case SO_MALFUNCTION:
	case SO_ONLINE:
	case SO_RECOVER:
		ref = so_dbunref(o, 0);
		if (ref > 0)
			return 0;
		/* set last visible transaction id */
		o->txn_max = sx_max(&e->xm);
		if (o->ctl.scheduled) {
			rc = so_scheduler_del(&e->sched, o);
			if (ssunlikely(rc == -1))
				return -1;
		}
		sr_objlist_del(&e->db, &o->o);
		ss_spinlock(&e->dblock);
		sr_objlist_add(&e->db_shutdown, &o->o);
		ss_spinunlock(&e->dblock);
		so_statusset(&o->status, SO_SHUTDOWN);
		return 0;
	case SO_SHUTDOWN:
		/* this intended to be called from a
		 * background gc task */
		assert(so_dbrefof(o, 0) == 0);
		ref = so_dbrefof(o, 1);
		if (ref > 0)
			return 0;
		goto shutdown;
	case SO_OFFLINE:
		sr_objlist_del(&e->db, &o->o);
		goto shutdown;
	default: assert(0);
	}

shutdown:;
	rc = sr_objlist_destroy(&o->cursor);
	if (ssunlikely(rc == -1))
		rcret = -1;
	sx_indexfree(&o->coindex, &e->xm);
	rc = si_close(&o->index, &o->r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	si_schemefree(&o->scheme, &o->r);
	sd_cfree(&o->dc, &o->r);
	so_statusfree(&o->status);
	ss_spinlockfree(&o->reflock);
	ss_free(&e->a_db, o);
	return rcret;
}

static int
so_dbdrop(srobj *obj, va_list args ssunused)
{
	sodb *o = (sodb*)obj;
	int status = so_status(&o->status);
	if (ssunlikely(! so_statusactive_is(status)))
		return -1;
	if (ssunlikely(o->ctl.dropped))
		return 0;
	int rc = si_dropmark(&o->index, &o->r);
	if (ssunlikely(rc == -1))
		return -1;
	o->ctl.dropped = 1;
	return 0;
}

static int
so_dberror(srobj *obj, va_list args ssunused)
{
	sodb *o = (sodb*)obj;
	int status = so_status(&o->status);
	if (status == SO_MALFUNCTION)
		return 1;
	return 0;
}

static int
so_dbset(srobj *obj, va_list args)
{
	sodb *o = (sodb*)obj;
	return so_txdbset(o, 0, 0, args);
}

static int
so_dbdel(srobj *obj, va_list args)
{
	sodb *o = (sodb*)obj;
	return so_txdbset(o, 0, SVDELETE, args);
}

static void*
so_dbget(srobj *obj, va_list args)
{
	sodb *o = (sodb*)obj;
	return so_txdbget(o, 0, 0, 1, args);
}

static void*
so_dbcursor(srobj *o, va_list args)
{
	sodb *db = (sodb*)o;
	return so_cursornew(db, 0, 0, args);
}

static void*
so_dbobj(srobj *obj, va_list args ssunused)
{
	sodb *o = (sodb*)obj;
	so *e = so_of(&o->o);
	return so_vnew(e, obj);
}

static void*
so_dbtype(srobj *o ssunused, va_list args ssunused) {
	return "database";
}

static srobjif sodbif =
{
	.ctl      = so_dbctl,
	.async    = so_dbasync,
	.open     = so_dbopen,
	.destroy  = so_dbdestroy,
	.error    = so_dberror,
	.set      = so_dbset,
	.del      = so_dbdel,
	.get      = so_dbget,
	.poll     = NULL,
	.drop     = so_dbdrop,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.cursor   = so_dbcursor,
	.object   = so_dbobj,
	.type     = so_dbtype
};

srobj *so_dbnew(so *e, char *name)
{
	sodb *o = ss_malloc(&e->a_db, sizeof(sodb));
	if (ssunlikely(o == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memset(o, 0, sizeof(*o));
	sr_objinit(&o->o, SODB, &sodbif, &e->o);
	sr_objlist_init(&o->cursor);
	so_statusinit(&o->status);
	so_statusset(&o->status, SO_OFFLINE);
	o->r        = e->r;
	o->r.scheme = &o->scheme.scheme;
	int rc = so_dbctl_init(&o->ctl, o);
	if (ssunlikely(rc == -1)) {
		ss_free(&e->a_db, o);
		return NULL;
	}
	rc = so_dbscheme_init(o, name);
	if (ssunlikely(rc == -1)) {
		ss_free(&e->a_db, o);
		return NULL;
	}
	so_dbasync_init(&o->async, o);
	rc = si_init(&o->index, &o->r, &e->quota);
	if (ssunlikely(rc == -1)) {
		ss_free(&e->a_db, o);
		si_schemefree(&o->scheme, &o->r);
		return NULL;
	}
	sx_indexinit(&o->coindex, o);
	ss_spinlockinit(&o->reflock);
	o->ref_be = 0;
	o->ref = 0;
	o->txn_min = sx_min(&e->xm);
	o->txn_max = o->txn_min;
	sd_cinit(&o->dc);
	return &o->o;
}

srobj *so_dbmatch(so *e, char *name)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sodb *db = (sodb*)sscast(i, srobj, link);
		if (strcmp(db->scheme.name, name) == 0)
			return &db->o;
	}
	return NULL;
}

srobj *so_dbmatch_id(so *e, uint32_t id)
{
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sodb *db = (sodb*)sscast(i, srobj, link);
		if (db->scheme.id == id)
			return &db->o;
	}
	return NULL;
}

void so_dbref(sodb *o, int be)
{
	ss_spinlock(&o->reflock);
	if (be)
		o->ref_be++;
	else
		o->ref++;
	ss_spinunlock(&o->reflock);
}

uint32_t so_dbunref(sodb *o, int be)
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

uint32_t so_dbrefof(sodb *o, int be)
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

int so_dbgarbage(sodb *o)
{
	ss_spinlock(&o->reflock);
	int v = o->ref_be == 0 && o->ref == 0;
	ss_spinunlock(&o->reflock);
	return v;
}

int so_dbvisible(sodb *db, uint32_t txn)
{
	return db->txn_min < txn && txn <= db->txn_max;
}

void so_dbbind(so *o)
{
	sslist *i;
	ss_listforeach(&o->db.list, i) {
		sodb *db = (sodb*)sscast(i, srobj, link);
		int status = so_status(&db->status);
		if (so_statusactive_is(status))
			so_dbref(db, 1);
	}
}

void so_dbunbind(so *o, uint32_t txn)
{
	sslist *i;
	ss_listforeach(&o->db.list, i) {
		sodb *db = (sodb*)sscast(i, srobj, link);
		int status = so_status(&db->status);
		if (status != SO_ONLINE)
			continue;
		if (txn > db->txn_min)
			so_dbunref(db, 1);
	}

	ss_spinlock(&o->dblock);
	ss_listforeach(&o->db_shutdown.list, i) {
		sodb *db = (sodb*)sscast(i, srobj, link);
		if (so_dbvisible(db, txn))
			so_dbunref(db, 1);
	}
	ss_spinunlock(&o->dblock);
}

int so_dbmalfunction(sodb *o)
{
	so_statusset(&o->status, SO_MALFUNCTION);
	return -1;
}

svv *so_dbv(sodb *db, sov *o, int search)
{
	so *e = so_of(&db->o);
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
