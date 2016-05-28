
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

static void
se_viewfree(so *o)
{
	se *e = se_of(o);
	seview *s = (seview*)o;
	ss_buffree(&s->name, &e->a);
	ss_free(&e->a, o);
}

static int
se_viewdestroy(so *o)
{
	seview *s = se_cast(o, seview*, SEVIEW);
	se *e = se_of(o);
	uint32_t id = s->t.id;
	se_dbunbind(e, id);
	if (sslikely(! s->db_view_only))
		sx_rollback(&s->t);
	ss_bufreset(&s->name);
	so_mark_destroyed(&s->o);
	so_poolgc(&e->view, o);
	return 0;
}

static void*
se_viewget(so *o, so *key)
{
	seview *s = se_cast(o, seview*, SEVIEW);
	se *e = se_of(o);
	sedocument *v = se_cast(key, sedocument*, SEDOCUMENT);
	sedb *db = se_cast(key->parent, sedb*, SEDB);
	if (s->db_view_only) {
		sr_error(&e->error, "view '%s' is in db-cursor-only mode", s->name);
		return NULL;
	}
	return se_dbread(db, v, NULL, s->t.vlsn, NULL);
}

static void*
se_viewcursor(so *o)
{
	seview *s = se_cast(o, seview*, SEVIEW);
	se *e = se_of(o);
	if (s->db_view_only) {
		sr_error(&e->error, "view '%s' is in db-view-only mode", s->name);
		return NULL;
	}
	return se_cursornew(e, s->vlsn);
}

static int
se_viewset_int(so *o, const char *path, int64_t v ssunused)
{
	seview *s = se_cast(o, seview*, SEVIEW);
	if (strcmp(path, "db-view-only") == 0) {
		if (s->db_view_only)
			return -1;
		sx_rollback(&s->t);
		s->db_view_only = 1;
		return 0;
	}
	return -1;
}

static soif seviewif =
{
	.open         = NULL,
	.close        = NULL,
	.destroy      = se_viewdestroy,
	.free         = se_viewfree,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = NULL,
	.setint       = se_viewset_int,
	.setobject    = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.upsert       = NULL,
	.del          = NULL,
	.get          = se_viewget,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = se_viewcursor
};

so *se_viewnew(se *e, uint64_t vlsn, char *name, int size)
{
	sslist *i;
	ss_listforeach(&e->view.list.list, i) {
		seview *s = (seview*)sscast(i, so, link);
		if (ssunlikely(strcmp(s->name.s, name) == 0)) {
			sr_error(&e->error, "view '%s' already exists", name);
			return NULL;
		}
	}
	seview *s = (seview*)so_poolpop(&e->view);
	int cache;
	if (! s) {
		s = ss_malloc(&e->a, sizeof(seview));
		cache = 0;
	} else {
		cache = 1;
	}
	if (ssunlikely(s == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&s->o, &se_o[SEVIEW], &seviewif, &e->o, &e->o);
	s->vlsn = vlsn;
	if (! cache)
		ss_bufinit(&s->name);
	int rc;
	if (size == 0)
		size = strlen(name);
	rc = ss_bufensure(&s->name, &e->a, size + 1);
	if (ssunlikely(rc == -1)) {
		so_mark_destroyed(&s->o);
		so_poolpush(&e->view, &s->o);
		sr_oom(&e->error);
		return NULL;
	}
	memcpy(s->name.s, name, size);
	s->name.s[size] = 0;
	ss_bufadvance(&s->name, size + 1);
	sv_loginit(&s->log);
	sx_begin(&e->xm, &s->t, SXRO, &s->log, vlsn);
	s->db_view_only = 0;
	se_dbbind(e);
	so_pooladd(&e->view, &s->o);
	return &s->o;
}

int se_viewupdate(seview *s)
{
	se *e = se_of(&s->o);
	uint32_t id = s->t.id;
	if (! s->db_view_only) {
		sx_rollback(&s->t);
		sx_begin(&e->xm, &s->t, SXRO, &s->log, s->vlsn);
	}
	s->t.id = id;
	return 0;
}
