
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libse.h>
#include <libso.h>

static int
so_snapshotdb_open(soobj *obj, va_list args)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	return o->db->i->open(o->db, args);
}

static int
so_snapshotdb_destroy(soobj *obj)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	int rcret = 0;
	int rc = so_objdestroy(o->db);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = so_objindex_destroy(&o->list);
	if (srunlikely(rc == -1))
		rcret = -1;
	sr_free(&o->e->a_snapshotdb, o);
	return rcret;
}

static int
so_snapshotdb_error(soobj *obj, va_list args)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	return o->db->i->error(o->db, args);
}

static inline sosnapshot*
so_snapshotdb_match(sosnapshotdb *db, char *name)
{
	srlist *i;
	sr_listforeach(&db->list.list, i) {
		sosnapshot *s = (sosnapshot*)srcast(i, soobj, link);
		if (srunlikely(strcmp(s->name, name) == 0))
			return s;
	}
	return NULL;
}

static int
so_snapshotdb_set(soobj *obj, va_list args)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	/* validate call */
	va_list va;
	va_copy(va, args);
	sov *v = va_arg(va, sov*);
	va_end(va);
	if (srunlikely(v->o.id != SOV))
		goto error;
	if (srunlikely(v->parent == NULL || v->parent->id != SODB))
		goto error;
	sodb *db = (sodb*)v->parent;
	if (&db->o != o->db)
		goto error;
	char *name = svkey(&v->v);
	if (srunlikely(name == NULL))
		goto error_release;
	if (srunlikely(svvalue(&v->v) == NULL ||
	               svvaluesize(&v->v) != sizeof(uint64_t)))
		goto error_release;
	uint64_t lsn = *(uint64_t*)svvalue(&v->v);
	/* ensure snapshot is not exists */
	sosnapshot *snapshot = so_snapshotdb_match(o, name);
	if (srunlikely(snapshot)) {
		so_objdestroy(&v->o);
		sr_error(&o->e->error, "snapshot '%s' already exists", name);
		sr_error_recoverable(&o->e->error);
		return -1;
	}
	int rc = o->db->i->set(o->db, args);
	if (srunlikely(rc == -1))
		return -1;
	/* todo: force checkpoint if logger is not used */
	/* create snapshot object */
	snapshot = (sosnapshot*)so_snapshotnew(o, lsn, name);
	if (srunlikely(snapshot == NULL))
		return -1;
	so_objindex_register(&o->list, &snapshot->o);
	return 0;
error_release:
	so_objdestroy(&v->o);
error:
	sr_error(&o->e->error, "%s", "bad arguments");
	sr_error_recoverable(&o->e->error);
	return -1;
}

static void*
so_snapshotdb_get(soobj *obj, va_list args)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	return o->db->i->get(o->db, args);
}

static int
so_snapshotdb_del(soobj *obj, va_list args)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	/* validate call */
	va_list va;
	va_copy(va, args);
	sov *v = va_arg(va, sov*);
	va_end(va);
	if (srunlikely(v->o.id != SOV))
		goto error;
	if (srunlikely(v->parent == NULL || v->parent->id != SODB))
		goto error;
	sodb *db = (sodb*)v->parent;
	if (&db->o != o->db)
		goto error;
	char *name = svkey(&v->v);
	if (srunlikely(name == NULL))
		goto error_release;
	/* match */
	sosnapshot *snapshot = (sosnapshot*)so_snapshotdb_match(o, name);
	if (srunlikely(snapshot == NULL)) {
		so_objdestroy(&v->o);
		sr_error(&o->e->error, "snapshot '%s' is not exist", name);
		sr_error_recoverable(&o->e->error);
		return -1;
	}
	/* delete snapshot key */
	int rc = o->db->i->del(o->db, args);
	if (srunlikely(rc == -1))
		return -1;
	/* todo: force checkpoint if logger is not used */
	/* note: snapshot will be self-freed */
	so_objindex_unregister(&o->list, &snapshot->o);
	return 0;
error_release:
	so_objdestroy(&v->o);
error:
	sr_error(&o->e->error, "%s", "bad arguments");
	sr_error_recoverable(&o->e->error);
	return -1;
}

static void*
so_snapshotdb_cursor(soobj *obj, va_list args)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	return o->db->i->cursor(o->db, args);
}

static void*
so_snapshotdb_obj(soobj *obj, va_list args)
{
	sosnapshotdb *o = (sosnapshotdb*)obj;
	return o->db->i->object(o->db, args);
}

static void*
so_snapshotdb_type(soobj *obj srunused, va_list args srunused) {
	return "snapshot_db";
}

static soobjif sosnapshotdbif =
{
	.ctl      = NULL,
	.open     = so_snapshotdb_open,
	.destroy  = so_snapshotdb_destroy,
	.error    = so_snapshotdb_error,
	.set      = so_snapshotdb_set,
	.get      = so_snapshotdb_get,
	.del      = so_snapshotdb_del,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = so_snapshotdb_cursor,
	.object   = so_snapshotdb_obj,
	.type     = so_snapshotdb_type
};

soobj *so_snapshotdb_new(so *e)
{
	sosnapshotdb *db = sr_malloc(&e->a_snapshotdb, sizeof(sosnapshotdb));
	if (srunlikely(db == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	so_objinit(&db->o, SOSNAPSHOTDB, &sosnapshotdbif, &e->o);
	db->e = e;
	db->db = so_dbnew(e, "snapshot");
	if (srunlikely(db->db == NULL)) {
		sr_free(&e->a_snapshotdb, db);
		return NULL;
	}
	so_objindex_init(&db->list);
	sodb *ptr = (sodb*)db->db;
	ptr->ctl.system = 1;
	return &db->o;
}
