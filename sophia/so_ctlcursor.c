
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libso.h>
#include <sophia.h>

static int
so_ctlcursor_destroy(soobj *o)
{
	soctlcursor *c = (soctlcursor*)o;
	so *e = c->e;
	sr_buffree(&c->dump, &e->a);
	if (c->v)
		sp_destroy(c->v);
	so_objindex_unregister(&e->ctlcursor, &c->o);
	sr_free(&e->a, c);
	return 0;
}

static inline int
so_ctlcursor_set(soctlcursor *c)
{
	int type = c->pos->type;
	srctl match = {
		.name = sr_ctldump_name(c->pos),
		.v    = sr_ctldump_value(c->pos),
		.type = type,
		.func = NULL
	};
	void *v = so_ctlreturn(&match, c->e);
	if (srunlikely(v == NULL))
		return -1;
	if (c->v)
		sp_destroy(c->v);
	c->v = v;
	return 0;
}

static inline int
so_ctlcursor_next(soctlcursor *c)
{
	int rc;
	if (c->pos == NULL) {
		assert( sr_bufsize(&c->dump) >= (int)sizeof(srctldump) );
		c->pos = (srctldump*)c->dump.s;
	} else {
		int size = sizeof(srctldump) + c->pos->namelen + c->pos->valuelen;
		c->pos = (srctldump*)((char*)c->pos + size);
		if ((char*)c->pos >= c->dump.p)
			c->pos = NULL;
	}
	if (srunlikely(c->pos == NULL)) {
		if (c->v)
			sp_destroy(c->v);
		c->v = NULL;
		return 0;
	}
	rc = so_ctlcursor_set(c);
	if (srunlikely(rc == -1))
		return -1;
	return 1;
}

static void*
so_ctlcursor_get(soobj *o, va_list args srunused)
{
	soctlcursor *c = (soctlcursor*)o;
	if (c->ready) {
		c->ready = 0;
		return c->v;
	}
	if (so_ctlcursor_next(c) == 0)
		return NULL;
	return c->v;
}

static void*
so_ctlcursor_obj(soobj *obj, va_list args srunused)
{
	soctlcursor *c = (soctlcursor*)obj;
	if (c->v == NULL)
		return NULL;
	return c->v;
}

static void*
so_ctlcursor_type(soobj *o srunused, va_list args srunused) {
	return "ctl_cursor";
}

static soobjif soctlcursorif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = so_ctlcursor_destroy,
	.set      = NULL,
	.get      = so_ctlcursor_get,
	.del      = NULL,
	.begin    = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = NULL,
	.object   = so_ctlcursor_obj,
	.type     = so_ctlcursor_type,
	.copy     = NULL
};

static inline int
so_ctlcursor_open(soctlcursor *c)
{
	so *e = c->e;
	int rc = so_ctldump(&e->ctl, &c->dump);
	if (srunlikely(rc == -1))
		return -1;
	rc = so_ctlcursor_next(c);
	if (srunlikely(rc == -1))
		return -1;
	c->ready = 1;
	return 0;
}

soobj *so_ctlcursor_new(void *o)
{
	so *e = o;
	soctlcursor *c = sr_malloc(&e->a, sizeof(soctlcursor));
	if (srunlikely(c == NULL))
		return NULL;
	so_objinit(&c->o, SOCTLCURSOR, &soctlcursorif);
	c->e = o;
	c->pos = NULL;
	c->v = NULL;
	c->ready = 0;
	sr_bufinit(&c->dump);
	int rc = so_ctlcursor_open(c);
	if (srunlikely(rc == -1)) {
		sp_destroy(c);
		return NULL;
	}
	so_objindex_unregister(&e->ctlcursor, &c->o);
	return &c->o;
}
