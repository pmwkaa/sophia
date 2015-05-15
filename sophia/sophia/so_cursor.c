
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
	sov *pref = (sov*)c->key;
	siquery q;
	si_queryopen(&q, &c->db->r, &c->cache,
	             &c->db->index, c->order, c->t.vlsn,
	             pref->prefix, pref->prefixsize,
	             key, keysize);
	int rc = si_query(&q);
	so_vrelease(&c->v);
	if (rc == 1) {
		assert(q.result.v != NULL);
		so_vput(&c->v, &q.result);
		so_vimmutable(&c->v);
	}
	si_queryclose(&q);
	return rc;
}

static int
so_cursordestroy(soobj *o, va_list args srunused)
{
	socursor *c = (socursor*)o;
	so *e = so_of(o);
	uint32_t id = c->t.id;
	sx_end(&c->t);
	si_cachefree(&c->cache, &c->db->r);
	if (c->key) {
		so_objdestroy(c->key);
		c->key = NULL;
	}
	so_vrelease(&c->v);
	so_objindex_unregister(&c->db->cursor, &c->o);
	so_dbunbind(e, id);
	sr_free(&e->a_cursor, c);
	return 0;
}

static void*
so_cursorget(soobj *o, va_list args srunused)
{
	socursor *c = (socursor*)o;
	if (srunlikely(c->ready)) {
		c->ready = 0;
		return &c->v;
	}
	if (srunlikely(c->order == SR_STOP))
		return 0;
	if (srunlikely(! so_vhas(&c->v)))
		return 0;
	int rc = so_cursorseek(c, sv_pointer(&c->v.v), sv_size(&c->v.v));
	if (srunlikely(rc <= 0))
		return NULL;
	return &c->v;
}

static void*
so_cursortype(soobj *o srunused, va_list args srunused) {
	return "cursor";
}

static soobjif socursorif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = so_cursordestroy,
	.error   = NULL,
	.set     = NULL,
	.get     = so_cursorget,
	.del     = NULL,
	.drop    = NULL,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = so_cursorobj,
	.type    = so_cursortype
};

soobj *so_cursornew(sodb *db, uint64_t vlsn, va_list args)
{
	so *e = so_of(&db->o);
	soobj *keyobj = va_arg(args, soobj*);

	/* validate call */
	sov *o = (sov*)keyobj;
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	soobj *parent = o->parent;
	if (srunlikely(parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return NULL;
	}

	/* prepare cursor */
	socursor *c = sr_malloc(&e->a_cursor, sizeof(socursor));
	if (srunlikely(c == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		goto error;
	}
	so_objinit(&c->o, SOCURSOR, &socursorif, &e->o);
	c->key   = keyobj;
	c->db    = db;
	c->ready = 0;
	c->order = o->order;
	so_vinit(&c->v, e, &db->o);
	si_cacheinit(&c->cache, &e->a_cursorcache);

	/* open cursor */
	uint32_t keysize = 0;
	void *key = NULL;
	svv *seek = NULL;
	if (o->keyc > 0) {
		/* search by key */
		seek = so_dbv(db, o, 1);
		if (srunlikely(seek == NULL))
			goto error;
		keysize = seek->size;
		key = sv_vpointer(seek);
	} else
	if (o->prefix)
	{
		/* search by prefix */
		if (sr_schemeof(&db->ctl.scheme, 0)->type == SR_STRING) {
			srfmtv fv;
			fv.key      = o->prefix;
			fv.r.size   = o->prefixsize;
			fv.r.offset = 0;
			seek = sv_vbuild(&e->r, &fv, 1, NULL, 0);
			if (srunlikely(seek == NULL))
				goto error;
			keysize = seek->size;
			key = sv_vpointer(seek);
		} else {
			sr_error(&e->error, "%s", "prefix search is only supported for a"
			         " first string-part");
			goto error;
		}
	}

	sx_begin(&e->xm, &c->t, vlsn);
	int rc = so_cursorseek(c, key, keysize);
	if (seek)
		sv_vfree(db->r.a, seek);
	if (srunlikely(rc == -1)) {
		sx_end(&c->t);
		goto error;
	}

	/* ensure correct iteration */
	srorder next = SR_GTE;
	switch (c->order) {
	case SR_LT:
	case SR_LTE: next = SR_LT;
		break;
	case SR_GT:
	case SR_GTE: next = SR_GT;
		break;
	default: assert(0);
	}
	c->order = next;
	if (rc == 1)
		c->ready = 1;

	so_dbbind(e);
	so_objindex_register(&db->cursor, &c->o);
	return &c->o;
error:
	if (keyobj)
		so_objdestroy(keyobj);
	if (c)
		sr_free(&e->a_cursor, c);
	return NULL;
}
