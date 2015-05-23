
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
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

void so_cursorend(socursor *c)
{
	so *e = so_of(&c->o);
	uint32_t id = c->t.id;
	if (c->cache)
		si_cachepool_push(c->cache);
	if (c->seek.v)
		sv_vfree(c->db->r.a, c->seek.v);
	so_vrelease(&c->v);
	so_objindex_unregister(&c->db->cursor, &c->o);
	so_dbunbind(e, id);
	sr_free(&e->a_cursor, c);
}

static int
so_cursordestroy(soobj *o, va_list args srunused)
{
	socursor *c = (socursor*)o;
	so *e = so_of(o);
	int shutdown = so_status(&e->status) == SO_SHUTDOWN;
	/* asynchronous */
	if (!shutdown && c->async) {
		sorequest *task = so_requestnew(e, SO_REQCURSORDESTROY, &c->o, &c->db->o);
		if (srunlikely(task == NULL))
			return -1;
		so_requestadd(e, task);
		return 0;
	}
	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQCURSORDESTROY, &c->o, &c->db->o);
	so_query(&req);
	so_cursorend(c);
	return 0;
}

static void*
so_cursorget(soobj *o, va_list args srunused)
{
	socursor *c = (socursor*)o;
	so *e = so_of(o);
	/* asynchronous */
	if (c->async) {
		sorequest *task = so_requestnew(e, SO_REQCURSORGET, &c->o, &c->db->o);
		if (srunlikely(task == NULL))
			return NULL;
		so_requestadd(e, task);
		return &c->o;
	}
	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQCURSORGET, &c->o, &c->db->o);
	so_query(&req);
	if (srunlikely(req.rc <= 0))
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
	.del     = NULL,
	.get     = so_cursorget,
	.poll    = NULL,
	.drop    = NULL,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = so_cursorobj,
	.type    = so_cursortype
};

soobj *so_cursornew(sodb *db, uint64_t vlsn, int async, va_list args)
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
	c->db     = db;
	c->async  = async;
	c->ready  = 0;
	c->order  = o->order;
	c->prefix = NULL;
	c->prefixsize = 0;
	c->cache  = NULL;
	sx_init(&e->xm, &c->t);
	so_vinit(&c->v, e, &db->o);

	/* allocate cursor cache */
	c->cache = si_cachepool_pop(&e->cachepool);
	if (srunlikely(c->cache == NULL))
		goto error;

	/* prepare key */
	svv *seek = NULL;
	if (o->keyc > 0) {
		/* search by key */
		seek = so_dbv(db, o, 1);
		if (srunlikely(seek == NULL))
			goto error;
	} else
	if (o->prefix)
	{
		/* search by prefix */
		if (sr_schemeof(&db->scheme.scheme, 0)->type == SR_STRING) {
			srfmtv fv;
			fv.key      = o->prefix;
			fv.r.size   = o->prefixsize;
			fv.r.offset = 0;
			seek = sv_vbuild(&e->r, &fv, 1, NULL, 0);
			if (srunlikely(seek == NULL))
				goto error;
			void *vptr = sv_vpointer(seek);
			c->prefix = sr_fmtkey(vptr, 0);
			c->prefixsize = sr_fmtkey_size(vptr, 0);
		} else {
			sr_error(&e->error, "%s", "prefix search is only supported for a"
			         " first string-part");
			goto error;
		}
	}
	so_objdestroy(keyobj);
	keyobj = NULL;
	sv_init(&c->seek, &sv_vif, seek, NULL);

	/* asynchronous */
	if (async) {
		sorequest *task = so_requestnew(e, SO_REQCURSOROPEN, &c->o, &db->o);
		if (srunlikely(task == NULL))
			goto error;
		sorequestarg *arg = &task->arg;
		arg->vlsn = vlsn;
		so_dbbind(e);
		so_objindex_register(&db->cursor, &c->o);
		so_requestadd(e, task);
		return &c->o;
	}

	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQCURSOROPEN, &c->o, &db->o);
	sorequestarg *arg = &req.arg;
	/* support (sync) snapshot read-view */
	arg->vlsn_generate = 0;
	arg->vlsn = vlsn;
	so_query(&req);
	if (srunlikely(req.rc == -1))
		goto error;
	so_dbbind(e);
	so_objindex_register(&db->cursor, &c->o);
	return &c->o;
error:
	if (keyobj)
		so_objdestroy(keyobj);
	if (c->cache)
		si_cachepool_push(c->cache);
	if (c)
		sr_free(&e->a_cursor, c);
	return NULL;
}
