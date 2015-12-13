
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
se_cursordestroy(so *o)
{
	secursor *c = se_cast(o, secursor*, SECURSOR);
	se *e = se_of(&c->o);
	uint64_t id = c->t.id;
	if (! c->read_commited)
		sx_rollback(&c->t);
	if (c->cache)
		si_cachepool_push(c->cache);
	so_listdel(&e->cursor, &c->o);
	se_dbunbind(e, id);
	sr_statcursor(&e->stat, c->start,
	              c->read_disk,
	              c->read_cache,
	              c->ops);
	se_mark_destroyed(&c->o);
	ss_free(&e->a_cursor, c);
	return 0;
}

static void*
se_cursorget(so *o, so *v)
{
	secursor *c = se_cast(o, secursor*, SECURSOR);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	sedb *db = se_cast(v->parent, sedb*, SEDB);
	ssorder order = key->order;
	if (ssunlikely(! key->orderset))
		order = SS_GTE;
	/* this statistics might be not complete, because
	 * last statement is not accounted here */
	c->read_disk  += key->read_disk;
	c->read_cache += key->read_cache;
	c->ops++;
	sx *x = &c->t;
	if (c->read_commited)
		x = NULL;
	return se_dbread(db, key, x, 0, c->cache, order);
}

static int
se_cursorset_int(so *o, const char *path, int64_t v)
{
	secursor *c = se_cast(o, secursor*, SECURSOR);
	if (strcmp(path, "read_commited") == 0) {
		if (c->read_commited)
			return -1;
		if (v != 1)
			return -1;
		sx_rollback(&c->t);
		c->read_commited = 1;
		return 0;
	}
	return -1;
}

static soif secursorif =
{
	.open         = NULL,
	.destroy      = se_cursordestroy,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = NULL,
	.setint       = se_cursorset_int,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.upsert       = NULL,
	.del          = NULL,
	.get          = se_cursorget,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_cursornew(se *e, uint64_t vlsn)
{
	secursor *c = ss_malloc(&e->a_cursor, sizeof(secursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&c->o, &se_o[SECURSOR], &secursorif, &e->o, &e->o);
	sx_init(&e->xm, &c->t);
	c->start = ss_utime();
	c->ops = 0;
	c->read_disk = 0;
	c->read_cache = 0;
	c->t.state = SXUNDEF;
	c->cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(c->cache == NULL)) {
		ss_free(&e->a_cursor, c);
		return NULL;
	}
	c->read_commited = 0;
	sx_begin(&e->xm, &c->t, SXRO, vlsn);
	se_dbbind(e);
	so_listadd(&e->cursor, &c->o);
	return &c->o;
}
