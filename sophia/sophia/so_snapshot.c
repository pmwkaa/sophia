
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
so_snapshotfree(sosnapshot *s)
{
	so *e = so_of(&s->o);
	sx_rollback(&s->t);
	if (srlikely(s->name)) {
		sr_free(&e->a, s->name);
		s->name = NULL;
	}
	sr_free(&e->a_snapshot, s);
	return 0;
}

static int
so_snapshotdestroy(soobj *o, va_list args srunused)
{
	sosnapshot *s = (sosnapshot*)o;
	so *e = so_of(o);
	uint32_t id = s->t.id;
	so_objindex_unregister(&e->snapshot, &s->o);
	so_dbunbind(e, id);
	so_snapshotfree(s);
	return 0;
}

static void*
so_snapshotget(soobj *o, va_list args)
{
	sosnapshot *s = (sosnapshot*)o;
	so *e = so_of(o);
	va_list va;
	va_copy(va, args);
	sov *v = va_arg(va, sov*);
	va_end(va);
	if (srunlikely(v->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	sodb *db = (sodb*)v->parent;
	return so_txdbget(db, 0, s->vlsn, 0, args);
}

static void*
so_snapshotcursor(soobj *o, va_list args)
{
	sosnapshot *s = (sosnapshot*)o;
	so *e = so_of(o);
	va_list va;
	va_copy(va, args);
	sov *v = va_arg(va, sov*);
	va_end(va);
	if (srunlikely(v->o.id != SOV))
		goto error;
	if (srunlikely(v->parent == NULL || v->parent->id != SODB))
		goto error;
	sodb *db = (sodb*)v->parent;
	return so_cursornew(db, s->vlsn, args);
error:
	sr_error(&e->error, "%s", "bad arguments");
	return NULL;
}

static void*
so_snapshottype(soobj *o srunused, va_list args srunused) {
	return "snapshot";
}

static soobjif sosnapshotif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = so_snapshotdestroy,
	.error   = NULL,
	.set     = NULL,
	.del     = NULL,
	.get     = so_snapshotget,
	.poll    = NULL,
	.drop    = so_snapshotdestroy,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = so_snapshotcursor,
	.object  = NULL,
	.type    = so_snapshottype
};

soobj *so_snapshotnew(so *e, uint64_t vlsn, char *name)
{
	srlist *i;
	sr_listforeach(&e->snapshot.list, i) {
		sosnapshot *s = (sosnapshot*)srcast(i, soobj, link);
		if (srunlikely(strcmp(s->name, name) == 0)) {
			sr_error(&e->error, "snapshot '%s' already exists", name);
			return NULL;
		}
	}
	sosnapshot *s = sr_malloc(&e->a_snapshot, sizeof(sosnapshot));
	if (srunlikely(s == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	so_objinit(&s->o, SOSNAPSHOT, &sosnapshotif, &e->o);
	s->vlsn = vlsn;
	s->name = sr_strdup(&e->a, name);
	if (srunlikely(s->name == NULL)) {
		sr_free(&e->a_snapshot, s);
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	sx_begin(&e->xm, &s->t, vlsn);
	so_dbbind(e);
	return &s->o;
}

int so_snapshotupdate(sosnapshot *s)
{
	so *e = so_of(&s->o);
	uint32_t id = s->t.id;
	sx_rollback(&s->t);
	sx_begin(&e->xm, &s->t, s->vlsn);
	s->t.id = id;
	return 0;
}
