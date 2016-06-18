
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
se_cursorfree(so *o)
{
	assert(o->destroyed);
	se *e = se_of(o);
	ss_free(&e->a, o);
}

static int
se_cursordestroy(so *o)
{
	secursor *c = se_cast(o, secursor*, SECURSOR);
	se *e = se_of(&c->o);
	sx_rollback(&c->t);
	if (c->cache)
		si_cachepool_push(c->cache);
	if (c->read_db) {
		sr_statcursor(&c->read_db->stat, c->start,
		              c->read_disk,
		              c->read_cache,
		              c->ops);
	}
	so_mark_destroyed(&c->o);
	so_poolgc(&e->cursor, &c->o);
	return 0;
}

static void*
se_cursorget(so *o, so *v)
{
	secursor *c = se_cast(o, secursor*, SECURSOR);
	sedocument *key = se_cast(v, sedocument*, SEDOCUMENT);
	sedb *db = se_cast(v->parent, sedb*, SEDB);
	if (ssunlikely(c->read_db == NULL))
		c->read_db = db;
	if (ssunlikely(! key->orderset))
		key->order = SS_GTE;
	sedocument *ret =
		(sedocument*)se_read(db, key, NULL, c->t.vlsn, c->cache);
	if (ret == NULL)
		return NULL;
	c->read_disk  += ret->read_disk;
	c->read_cache += ret->read_cache;
	c->ops++;
	return ret;
}

static soif secursorif =
{
	.open         = NULL,
	.destroy      = se_cursordestroy,
	.free         = se_cursorfree,
	.document     = NULL,
	.setstring    = NULL,
	.setint       = NULL,
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
	secursor *c = (secursor*)so_poolpop(&e->cursor);
	if (c == NULL)
		c = ss_malloc(&e->a, sizeof(secursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&c->o, &se_o[SECURSOR], &secursorif, &e->o, &e->o);
	sv_loginit(&c->log);
	sx_init(&e->xm, &c->t, &c->log);
	c->start = ss_utime();
	c->ops = 0;
	c->read_disk = 0;
	c->read_cache = 0;
	c->read_db = NULL;
	c->t.state = SX_UNDEF;
	c->cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(c->cache == NULL)) {
		so_mark_destroyed(&c->o);
		so_poolpush(&e->cursor, &c->o);
		sr_oom(&e->error);
		return NULL;
	}
	sx_begin(&e->xm, &c->t, SX_RO, &c->log, vlsn);
	so_pooladd(&e->cursor, &c->o);
	return &c->o;
}
