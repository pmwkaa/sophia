
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
se_viewdb_free(so *o)
{
	seviewdb *c = (seviewdb*)o;
	se *e = se_of(&c->o);
	ss_buffree(&c->list, &e->a);
	ss_free(&e->a, c);
}

static int
se_viewdb_destroy(so *o, int fe ssunused)
{
	seviewdb *c = se_cast(o, seviewdb*, SEDBCURSOR);
	se *e = se_of(&c->o);
	ss_bufreset(&c->list);
	so_mark_destroyed(&c->o);
	so_poolgc(&e->viewdb, &c->o);
	return 0;
}

static void*
se_viewdb_get(so *o, so *v ssunused)
{
	seviewdb *c = se_cast(o, seviewdb*, SEDBCURSOR);
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

static soif seviewdbif =
{
	.open         = NULL,
	.close        = NULL,
	.destroy      = se_viewdb_destroy,
	.free         = se_viewdb_free,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.upsert       = NULL,
	.del          = NULL,
	.get          = se_viewdb_get,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

static inline int
se_viewdb_open(seviewdb *c)
{
	se *e = se_of(&c->o);
	int rc;
	sslist *i;
	ss_listforeach(&e->db.list, i) {
		sedb *db = (sedb*)sscast(i, so, link);
		int status = sr_status(&db->index.status);
		if (status != SR_ONLINE)
			continue;
		if (se_dbvisible(db, c->txn_id)) {
			rc = ss_bufadd(&c->list, &e->a, &db, sizeof(db));
			if (ssunlikely(rc == -1))
				return -1;
		}
	}
	if (ss_bufsize(&c->list) == 0)
		return 0;
	c->ready = 1;
	c->pos = c->list.s;
	c->v = *(sedb**)c->list.s;
	return 0;
}

so *se_viewdb_new(se *e, uint64_t txn_id)
{
	int cache;
	seviewdb *c = (seviewdb*)so_poolpop(&e->viewdb);
	if (! c) {
		cache = 0;
		c = ss_malloc(&e->a, sizeof(seviewdb));
	} else {
		cache = 1;
	}
	if (c == NULL)
		c = ss_malloc(&e->a, sizeof(seviewdb));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&c->o, &se_o[SEDBCURSOR], &seviewdbif,
	        &e->o, &e->o);
	c->txn_id = txn_id;
	c->v      = NULL;
	c->pos    = NULL;
	c->ready  = 0;
	if (! cache)
		ss_bufinit(&c->list);
	int rc = se_viewdb_open(c);
	if (ssunlikely(rc == -1)) {
		so_mark_destroyed(&c->o);
		so_poolpush(&e->viewdb, &c->o);
		sr_oom(&e->error);
		return NULL;
	}
	so_pooladd(&e->viewdb, &c->o);
	return &c->o;
}
