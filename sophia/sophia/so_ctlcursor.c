
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
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libse.h>
#include <libso.h>

static int
so_ctlcursor_destroy(srobj *o, va_list args ssunused)
{
	soctlcursor *c = (soctlcursor*)o;
	so *e = so_of(o);
	ss_buffree(&c->dump, &e->a);
	sr_objlist_del(&e->ctlcursor, &c->o);
	ss_free(&e->a_ctlcursor, c);
	return 0;
}

static inline srobj*
so_ctlcursor_object(soctlcursor *c)
{
	int type = c->pos->type;
	void *value = NULL;
	if (c->pos->valuelen > 0)
		value = sr_cvvalue(c->pos);
	src match = {
		.name     = sr_cvname(c->pos),
		.value    = value,
		.flags    = type,
		.ptr      = NULL,
		.function = NULL,
		.next     = NULL
	};
	so *e = so_of(&c->o);
	return so_ctlreturn(&match, e);
}

static void*
so_ctlcursor_get(srobj *o, va_list args ssunused)
{
	soctlcursor *c = (soctlcursor*)o;
	if (c->first) {
		assert( ss_bufsize(&c->dump) >= (int)sizeof(srcv) );
		c->first = 0;
		c->pos = (srcv*)c->dump.s;
	} else {
		int size = sizeof(srcv) + c->pos->namelen + c->pos->valuelen;
		c->pos = (srcv*)((char*)c->pos + size);
		if ((char*)c->pos >= c->dump.p)
			c->pos = NULL;
	}
	if (ssunlikely(c->pos == NULL))
		return NULL;
	return so_ctlcursor_object(c);
}

static void*
so_ctlcursor_type(srobj *o ssunused, va_list args ssunused) {
	return "ctl_cursor";
}

static srobjif soctlcursorif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = so_ctlcursor_destroy,
	.error   = NULL,
	.set     = NULL,
	.del     = NULL,
	.get     = so_ctlcursor_get,
	.poll    = NULL,
	.drop    = NULL,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = NULL,
	.type    = so_ctlcursor_type
};

srobj *so_ctlcursor_new(void *o)
{
	so *e = o;
	soctlcursor *c = ss_malloc(&e->a_ctlcursor, sizeof(soctlcursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	sr_objinit(&c->o, SOCTLCURSOR, &soctlcursorif, &e->o);
	c->pos = NULL;
	c->first = 1;
	ss_bufinit(&c->dump);
	int rc = so_ctlserialize(&e->ctl, &c->dump);
	if (ssunlikely(rc == -1)) {
		sr_objdestroy(&c->o);
		return NULL;
	}
	sr_objlist_add(&e->ctlcursor, &c->o);
	return &c->o;
}
