
/*
 * sophia databaso
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD Licenso
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libse.h>
#include <libso.h>

static void*
so_cursorobj(soobj *obj, va_list args srunused)
{
	socursor *c = (socursor*)obj;
	if (srunlikely(! so_vhas(&c->v)))
		return NULL;
	return &c->v;
}

static inline int
so_cursorseek(socursor *c, void *key, int keysize)
{
	siquery q;
	si_queryopen(&q, &c->db->r, &c->db->index, c->order,
	             c->t.vlsn, key, keysize);
	si_query(&q);
	so_vrelease(&c->v);
	if (q.result.v) {
		sv result;
		int rc = si_querydup(&q, &result);
		if (srunlikely(rc == -1)) {
			si_queryclose(&q);
			return -1;
		}
		so_vput(&c->v, &result);
		so_vimmutable(&c->v);
	}
	si_queryclose(&q);
	return so_vhas(&c->v);
}

static inline int
so_cursoropen(socursor *c, void *key, int keysize)
{
	sx_begin(&c->db->e->xm, &c->t);
	int rc;
	do {
		rc = so_cursorseek(c, key, keysize);
	} while (rc == 1 && (svflags(&c->v.v) & SVDELETE) > 0);

	if (srunlikely(rc == -1)) {
		sx_end(&c->t);
		return -1;
	}
	return so_vhas(&c->v);
}

static int
so_cursordestroy(soobj *o)
{
	socursor *c = (socursor*)o;
	sx_end(&c->t);
	if (c->key) {
		so_objdestroy(c->key);
		c->key = NULL;
	}
	so_vrelease(&c->v);
	so_objindex_unregister(&c->db->cursor, &c->o);
	sr_free(&c->db->e->a_cursor, c);
	return 0;
}

static inline int
so_cursorfetch(soobj *o)
{
	socursor *c = (socursor*)o;
	if (srunlikely(c->ready)) {
		c->ready = 0;
		return so_vhas(&c->v);
	}
	if (srunlikely(c->order == SR_STOP))
		return 0;
	if (srunlikely(! so_vhas(&c->v)))
		return 0;
	return so_cursorseek(c, svkey(&c->v.v), svkeysize(&c->v.v));
}

static void*
so_cursorget(soobj *o, va_list args srunused)
{
	socursor *c = (socursor*)o;
	int rc;
	do {
		rc = so_cursorfetch(o);
	} while (rc == 1 && (svflags(&c->v.v) & SVDELETE) > 0);
	if (srunlikely(rc == 0))
		return NULL;
	return &c->v;
}

static void*
so_cursortype(soobj *o srunused, va_list args srunused) {
	return "cursor";
}

static soobjif socursorif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = so_cursordestroy,
	.error    = NULL,
	.set      = NULL,
	.get      = so_cursorget,
	.del      = NULL,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = NULL,
	.object   = so_cursorobj,
	.type     = so_cursortype
};

soobj *so_cursornew(sodb *db, va_list args)
{
	so *e = db->e;
	char *order = va_arg(args, char*);
	soobj *keyobj = va_arg(args, soobj*);
	socursor *c = NULL;

	/* prepare cursor */
	srorder cmp;
	if (strcmp(order, ">") == 0) {
		cmp = SR_GT;
	} else
	if (strcmp(order, ">=") == 0) {
		cmp = SR_GTE;
	} else
	if (strcmp(order, "<") == 0) {
		cmp = SR_LT;
	} else
	if (strcmp(order, "<=") == 0) {
		cmp = SR_LTE;
	} else
	if (strcmp(order, "random") == 0) {
		cmp = SR_RANDOM;
	} else {
		goto error;
	}
	c = sr_malloc(&e->a_cursor, sizeof(socursor));
	if (srunlikely(c == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		goto error;
	}
	so_objinit(&c->o, SOCURSOR, &socursorif, &e->o);
	c->key   = keyobj;
	c->db    = db;
	c->ready = 1;
	c->order = cmp;
	so_vinit(&c->v, e, &db->o);

	/* open cursor */
	void *key = NULL;
	uint32_t keysize = 0;
	sv *ov = NULL;
	if (srunlikely(keyobj->id != SOV))
		goto error;
	ov = &((sov*)keyobj)->v;
	keysize = svkeysize(ov);
	key = svkey(ov);
	if (keysize == 0)
		key = NULL;
	int rc = so_cursoropen(c, key, keysize);
	if (srunlikely(rc == -1))
		goto error;

	/* prepare for iterations */
	srorder o = SR_GTE;
	switch (c->order) {
	case SR_LT:
	case SR_LTE:    o = SR_LT;
		break;
	case SR_GT:
	case SR_GTE:    o = SR_GT;
		break;
	case SR_RANDOM: o = SR_STOP;
		break;
	default: assert(0);
	}
	c->order = o;
	so_objindex_register(&db->cursor, &c->o);
	return &c->o;

error:
	if (keyobj)
		so_objdestroy(keyobj);
	if (c)
		sr_free(&e->a_cursor, c);
	return NULL;
}
