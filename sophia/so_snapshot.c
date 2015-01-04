
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
	sx_end(&s->t);
	if (srlikely(s->name)) {
		sr_free(&s->e->a, s->name);
		s->name = NULL;
	}
	sr_free(&s->e->a_snapshot, s);
	return 0;
}

static int
so_snapshotdestroy(soobj *o)
{
	sosnapshot *s = (sosnapshot*)o;
	so *e = s->e;
	int status = so_status(&e->status);
	if (status != SO_SHUTDOWN)
		return 0;
	so_snapshotfree(s);
	return 0;
}

static int
so_snapshotdelete(soobj *o, va_list args srunused)
{
	sosnapshot *s = (sosnapshot*)o;
	so_objindex_unregister(&s->e->snapshot, &s->o);
	so_snapshotfree(s);
	return 0;
}

static void*
so_snapshotget(soobj *o, va_list args)
{
	sosnapshot *s = (sosnapshot*)o;
	va_list va;
	va_copy(va, args);
	sov *v = va_arg(va, sov*);
	va_end(va);
	if (srunlikely(v->o.id != SOV)) {
		sr_error(&s->e->error, "%s", "bad arguments");
		return NULL;
	}
	sodb *db = (sodb*)v->parent;
	return so_txdbget(db, s->vlsn, args);
}

static void*
so_snapshotcursor(soobj *o, va_list args)
{
	sosnapshot *s = (sosnapshot*)o;
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
	sr_error(&s->e->error, "%s", "bad arguments");
	return NULL;
}

static void*
so_snapshottype(soobj *o srunused, va_list args srunused) {
	return "snapshot";
}

static soobjif sosnapshotif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = so_snapshotdestroy,
	.error    = NULL,
	.set      = NULL,
	.get      = so_snapshotget,
	.del      = so_snapshotdelete,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = so_snapshotcursor,
	.object   = NULL,
	.type     = so_snapshottype
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
	s->e = e;
	s->vlsn = vlsn;
	s->name = sr_strdup(&e->a, name);
	if (srunlikely(s->name == NULL)) {
		sr_free(&e->a_snapshot, s);
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	sx_begin(&e->xm, &s->t, vlsn);
	return &s->o;
}

int so_snapshotupdate(sosnapshot *s)
{
	sx_end(&s->t);
	sx_begin(&s->e->xm, &s->t, s->vlsn);
	return 0;
}
