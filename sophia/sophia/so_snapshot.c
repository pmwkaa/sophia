
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

static int
so_snapshotfree(sosnapshot *s)
{
	so *e = so_of(&s->o);
	sx_rollback(&s->t);
	if (sslikely(s->name)) {
		ss_free(&e->a, s->name);
		s->name = NULL;
	}
	ss_free(&e->a_snapshot, s);
	return 0;
}

static int
so_snapshotdestroy(srobj *o, va_list args ssunused)
{
	sosnapshot *s = (sosnapshot*)o;
	so *e = so_of(o);
	uint32_t id = s->t.id;
	sr_objlist_del(&e->snapshot, &s->o);
	so_dbunbind(e, id);
	so_snapshotfree(s);
	return 0;
}

static void*
so_snapshotget(srobj *o, va_list args)
{
	sosnapshot *s = (sosnapshot*)o;
	so *e = so_of(o);
	va_list va;
	va_copy(va, args);
	sov *v = va_arg(va, sov*);
	va_end(va);
	if (ssunlikely(v->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	sodb *db = (sodb*)v->parent;
	return so_txdbget(db, 0, s->vlsn, 0, args);
}

static void*
so_snapshotcursor(srobj *o, va_list args)
{
	sosnapshot *s = (sosnapshot*)o;
	so *e = so_of(o);
	va_list va;
	va_copy(va, args);
	sov *v = va_arg(va, sov*);
	va_end(va);
	if (ssunlikely(v->o.id != SOV))
		goto error;
	if (ssunlikely(v->parent == NULL || v->parent->id != SODB))
		goto error;
	sodb *db = (sodb*)v->parent;
	return so_cursornew(db, s->vlsn, 0, args);
error:
	sr_error(&e->error, "%s", "bad arguments");
	return NULL;
}

static void*
so_snapshottype(srobj *o ssunused, va_list args ssunused) {
	return "snapshot";
}

static srobjif sosnapshotif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = so_snapshotdestroy,
	.error   = NULL,
	.set     = NULL,
	.update  = NULL,
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

srobj *so_snapshotnew(so *e, uint64_t vlsn, char *name)
{
	sslist *i;
	ss_listforeach(&e->snapshot.list, i) {
		sosnapshot *s = (sosnapshot*)sscast(i, srobj, link);
		if (ssunlikely(strcmp(s->name, name) == 0)) {
			sr_error(&e->error, "snapshot '%s' already exists", name);
			return NULL;
		}
	}
	sosnapshot *s = ss_malloc(&e->a_snapshot, sizeof(sosnapshot));
	if (ssunlikely(s == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	sr_objinit(&s->o, SOSNAPSHOT, &sosnapshotif, &e->o);
	s->vlsn = vlsn;
	s->name = ss_strdup(&e->a, name);
	if (ssunlikely(s->name == NULL)) {
		ss_free(&e->a_snapshot, s);
		sr_oom(&e->error);
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
