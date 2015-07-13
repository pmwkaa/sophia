
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
se_snapshotfree(sesnapshot *s)
{
	se *e = se_of(&s->o);
	sx_rollback(&s->t);
	if (sslikely(s->name)) {
		ss_free(&e->a, s->name);
		s->name = NULL;
	}
	ss_free(&e->a_snapshot, s);
	return 0;
}

static int
se_snapshotdestroy(so *o)
{
	sesnapshot *s = se_cast(o, sesnapshot*, SESNAPSHOT);
	se *e = se_of(o);
	uint32_t id = s->t.id;
	so_listdel(&e->snapshot, &s->o);
	se_dbunbind(e, id);
	se_snapshotfree(s);
	return 0;
}

static void*
se_snapshotget(so *o, so *key)
{
	sesnapshot *s = se_cast(o, sesnapshot*, SESNAPSHOT);
	sev *v = se_cast(key, sev*, SEV);
	sedb *db = se_cast(key->parent, sedb*, SEDB);
	return se_txdbget(db, v, 0, s->vlsn, 0);
}

static void*
se_snapshotcursor(so *o, so *key)
{
	sesnapshot *s = (sesnapshot*)o;
	sev *v = se_cast(key, sev*, SEV);
	sedb *db = se_cast(key->parent, sedb*, SEDB);
	return se_cursornew(db, v, s->vlsn, 0);
}

static soif sesnapshotif =
{
	.open         = NULL,
	.destroy      = se_snapshotdestroy,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = se_snapshotget,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = se_snapshotcursor
};

so *se_snapshotnew(se *e, uint64_t vlsn, char *name)
{
	sslist *i;
	ss_listforeach(&e->snapshot.list, i) {
		sesnapshot *s = (sesnapshot*)sscast(i, so, link);
		if (ssunlikely(strcmp(s->name, name) == 0)) {
			sr_error(&e->error, "snapshot '%s' already exists", name);
			return NULL;
		}
	}
	sesnapshot *s = ss_malloc(&e->a_snapshot, sizeof(sesnapshot));
	if (ssunlikely(s == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&s->o, &se_o[SESNAPSHOT], &sesnapshotif, &e->o, &e->o);
	s->vlsn = vlsn;
	s->name = ss_strdup(&e->a, name);
	if (ssunlikely(s->name == NULL)) {
		ss_free(&e->a_snapshot, s);
		sr_oom(&e->error);
		return NULL;
	}
	sx_begin(&e->xm, &s->t, vlsn);
	se_dbbind(e);
	return &s->o;
}

int se_snapshotupdate(sesnapshot *s)
{
	se *e = se_of(&s->o);
	uint32_t id = s->t.id;
	sx_rollback(&s->t);
	sx_begin(&e->xm, &s->t, s->vlsn);
	s->t.id = id;
	return 0;
}
