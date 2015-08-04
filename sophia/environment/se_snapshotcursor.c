
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
se_snapshotcursor_destroy(so *o)
{
	sesnapshotcursor *c =
		se_cast(o, sesnapshotcursor*, SESNAPSHOTCURSOR);
	se *e = se_of(&c->o);
	ss_buffree(&c->list, &e->a);
	so_listdel(&c->s->cursor, &c->o);
	se_mark_destroyed(&c->o);
	ss_free(&e->a_snapshotcursor, c);
	return 0;
}

static void*
se_snapshotcursor_get(so *o, so *v ssunused)
{
	sesnapshotcursor *c =
		se_cast(o, sesnapshotcursor*, SESNAPSHOTCURSOR);
	if (c->ready) {
		c->ready = 0;
		return c->v;
	}
	if (ssunlikely(c->pos == NULL))
		return NULL;
	c->pos += sizeof(sedb**);
	if (c->pos >= c->list.p) {
		c->pos = NULL;
		c->v = NULL;
		return NULL;
	}
	c->v = *(sedb**)c->pos;
	return c->v;
}

static soif sesnapshotcursorif =
{
	.open         = NULL,
	.destroy      = se_snapshotcursor_destroy,
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
	.get          = se_snapshotcursor_get,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

static inline int
se_snapshotcursor_open(sesnapshotcursor *c)
{
	se *e = se_of(&c->o);
	int rc;
	uint32_t txn = c->s->t.id;
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		int status = se_status(&db->status);
		if (status != SE_ONLINE)
			continue;
		if (txn > db->txn_min) {
			rc = ss_bufadd(&c->list, &e->a, &db, sizeof(db));
			if (ssunlikely(rc == -1))
				return -1;
		}
	}
	ss_spinlock(&e->dblock);
	ss_listforeach(&e->db_shutdown.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		if (db->txn_min < txn && txn <= db->txn_max) {
			rc = ss_bufadd(&c->list, &e->a, &db, sizeof(db));
			if (ssunlikely(rc == -1))
				return -1;
		}
	}
	ss_spinunlock(&e->dblock);
	if (ss_bufsize(&c->list) == 0)
		return 0;
	c->ready = 1;
	c->pos = c->list.s;
	c->v = *(sedb**)c->list.s;
	return 0;
}

so *se_snapshotcursor_new(sesnapshot *s)
{
	se *e = se_of(&s->o);
	sesnapshotcursor *c =
		ss_malloc(&e->a_snapshotcursor, sizeof(sesnapshotcursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&c->o, &se_o[SESNAPSHOTCURSOR],
	        &sesnapshotcursorif, &e->o, &e->o);
	c->s = s;
	c->v = NULL;
	c->pos = NULL;
	c->ready = 0;
	ss_bufinit(&c->list);
	int rc = se_snapshotcursor_open(c);
	if (ssunlikely(rc == -1)) {
		so_destroy(&c->o);
		sr_oom(&e->error);
		return NULL;
	}
	so_listadd(&s->cursor, &c->o);
	return &c->o;
}
