
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
	so_objindex_unregister(&s->e->snapshot, &s->o);
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
	so *e = s->e;
	int rc = se_snapshot_remove(&e->se, &e->r, s->name);
	if (srunlikely(rc == -1))
		return -1;
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
		sr_error_recoverable(&s->e->error);
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
	char *order = va_arg(va, char*);
	(void)order;
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
	sr_error_recoverable(&s->e->error);
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

soobj *so_snapshotnew(so *e, int create, uint64_t vlsn, char *name)
{
	if (create) {
		int rc = se_snapshot(&e->se, &e->r, vlsn, name);
		if (srunlikely(rc == -1))
			return NULL;
	}
	sosnapshot *s = sr_malloc(&e->a_snapshot, sizeof(sosnapshot));
	if (srunlikely(s == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	so_objinit(&s->o, SOSNAPSHOT, &sosnapshotif, &e->o);
	s->e = e;
	s->vlsn = vlsn;
	s->name = sr_strdup(&e->a, name);
	if (srunlikely(s->name == NULL)) {
		sr_free(&e->a_snapshot, s);
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	sx_begin(&e->xm, &s->t, vlsn);
	so_objindex_register(&e->snapshot, &s->o);
	return &s->o;
}
