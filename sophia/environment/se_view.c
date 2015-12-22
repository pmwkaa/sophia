
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
se_viewfree(seview *s)
{
	se *e = se_of(&s->o);
	if (sslikely(! s->db_view_only))
		sx_rollback(&s->t);
	if (sslikely(s->name)) {
		ss_free(&e->a, s->name);
		s->name = NULL;
	}
	se_mark_destroyed(&s->o);
	ss_free(&e->a_view, s);
	return 0;
}

static int
se_viewdestroy(so *o)
{
	seview *s = se_cast(o, seview*, SEVIEW);
	se *e = se_of(o);
	so_listdestroy(&s->cursor);
	uint32_t id = s->t.id;
	so_listdel(&e->view, &s->o);
	se_dbunbind(e, id);
	se_viewfree(s);
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
	return se_dbread(db, v, &s->t, 0, NULL, SS_EQ);
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

void *se_viewget_object(so *o, const char *path)
{
	seview *s = se_cast(o, seview*, SEVIEW);
	se *e = se_of(o);
	if (strcmp(path, "db") == 0)
		return se_viewdb_new(e, s->t.id);
	return NULL;
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
	.destroy      = se_viewdestroy,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = NULL,
	.setint       = se_viewset_int,
	.getobject    = se_viewget_object,
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

so *se_viewnew(se *e, uint64_t vlsn, char *name)
{
	sslist *i;
	ss_listforeach(&e->view.list, i) {
		seview *s = (seview*)sscast(i, so, link);
		if (ssunlikely(strcmp(s->name, name) == 0)) {
			sr_error(&e->error, "view '%s' already exists", name);
			return NULL;
		}
	}
	seview *s = ss_malloc(&e->a_view, sizeof(seview));
	if (ssunlikely(s == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&s->o, &se_o[SEVIEW], &seviewif, &e->o, &e->o);
	so_listinit(&s->cursor);
	s->vlsn = vlsn;
	s->name = ss_strdup(&e->a, name);
	if (ssunlikely(s->name == NULL)) {
		ss_free(&e->a_view, s);
		sr_oom(&e->error);
		return NULL;
	}
	sx_begin(&e->xm, &s->t, SXRO, vlsn);
	s->db_view_only = 0;
	se_dbbind(e);
	return &s->o;
}

int se_viewupdate(seview *s)
{
	se *e = se_of(&s->o);
	uint32_t id = s->t.id;
	if (! s->db_view_only) {
		sx_rollback(&s->t);
		sx_begin(&e->xm, &s->t, SXRO, s->vlsn);
	}
	s->t.id = id;
	return 0;
}
