
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

void so_cursorend(socursor *c)
{
	so *e = so_of(&c->o);
	uint32_t id = c->t.id;
	if (c->cache)
		si_cachepool_push(c->cache);
	if (c->seek.v)
		sv_vfree(c->db->r.a, c->seek.v);
	if (c->v.v)
		sv_vfree(c->db->r.a, c->v.v);
	sr_objlist_del(&c->db->cursor, &c->o);
	so_dbunbind(e, id);
	ss_free(&e->a_cursor, c);
}

static int
so_cursordestroy(srobj *o, va_list args ssunused)
{
	socursor *c = (socursor*)o;
	so *e = so_of(o);
	int shutdown = so_status(&e->status) == SO_SHUTDOWN;
	/* asynchronous */
	if (!shutdown && c->async) {
		sorequest *task = so_requestnew(e, SO_REQCURSORDESTROY, &c->o, &c->db->o);
		if (ssunlikely(task == NULL))
			return -1;
		so_requestadd(e, task);
		return 0;
	}
	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQCURSORDESTROY, &c->o, &c->db->o);
	so_query(&req);
	so_requestend(&req);
	so_cursorend(c);
	return 0;
}

static void*
so_cursorget(srobj *o, va_list args ssunused)
{
	socursor *c = (socursor*)o;
	so *e = so_of(o);
	/* asynchronous */
	if (c->async) {
		sorequest *task = so_requestnew(e, SO_REQCURSORGET, &c->o, &c->db->o);
		if (ssunlikely(task == NULL))
			return NULL;
		sorequestarg *arg = &task->arg;
		arg->order = c->order;
		arg->vlsn_generate = 0;
		arg->vlsn = c->t.vlsn;
		arg->prefix = c->prefix;
		arg->prefixsize = c->prefixsize;
		so_requestadd(e, task);
		return &task->o;
	}
	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQCURSORGET, &c->o, &c->db->o);
	sorequestarg *arg = &req.arg;
	arg->order = c->order;
	arg->vlsn_generate = 0;
	arg->vlsn = c->t.vlsn;
	arg->prefix = c->prefix;
	arg->prefixsize = c->prefixsize;
	int rc = so_query(&req);
	so_dbunref(c->db, 1);
	if (rc == 1)
		so_requestresult(&req);
	return req.result;
}

static void*
so_cursortype(srobj *o ssunused, va_list args ssunused) {
	return "cursor";
}

static srobjif socursorif =
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
	.object  = NULL,
	.type    = so_cursortype
};

srobj *so_cursornew(sodb *db, uint64_t vlsn, int async, va_list args)
{
	so *e = so_of(&db->o);
	srobj *keyobj = va_arg(args, srobj*);

	/* validate call */
	sov *o = (sov*)keyobj;
	if (ssunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	srobj *parent = o->parent;
	if (ssunlikely(parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return NULL;
	}

	/* prepare cursor */
	socursor *c = ss_malloc(&e->a_cursor, sizeof(socursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		goto error;
	}
	sr_objinit(&c->o, SOCURSOR, &socursorif, &e->o);
	c->db         = db;
	c->async      = async;
	c->ready      = 0;
	c->order      = o->order;
	c->prefix     = NULL;
	c->prefixsize = 0;
	c->cache      = NULL;
	memset(&c->v, 0, sizeof(c->v));
	sx_init(&e->xm, &c->t);

	/* allocate cursor cache */
	c->cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(c->cache == NULL))
		goto error;

	/* prepare key */
	svv *seek = NULL;
	if (o->keyc > 0) {
		/* search by key */
		seek = so_dbv(db, o, 1);
		if (ssunlikely(seek == NULL))
			goto error;
	} else
	if (o->prefix)
	{
		/* search by prefix */
		if (sr_schemeof(&db->scheme.scheme, 0)->type == SS_STRING) {
			sfv fv;
			fv.key      = o->prefix;
			fv.r.size   = o->prefixsize;
			fv.r.offset = 0;
			seek = sv_vbuild(&e->r, &fv, 1, NULL, 0);
			if (ssunlikely(seek == NULL))
				goto error;
			void *vptr = sv_vpointer(seek);
			c->prefix = sf_key(vptr, 0);
			c->prefixsize = sf_keysize(vptr, 0);
		} else {
			sr_error(&e->error, "%s", "prefix search is only supported for a"
			         " first string-part");
			goto error;
		}
	}
	sr_objdestroy(keyobj);
	keyobj = NULL;
	sv_init(&c->seek, &sv_vif, seek, NULL);

	/* asynchronous */
	if (async) {
		sorequest *task = so_requestnew(e, SO_REQCURSOROPEN, &c->o, &db->o);
		if (ssunlikely(task == NULL))
			goto error;
		sorequestarg *arg = &task->arg;
		arg->order = c->order;
		arg->vlsn_generate = 0;
		arg->vlsn = vlsn;
		arg->prefix = c->prefix;
		arg->prefixsize = c->prefixsize;
		so_dbbind(e);
		sr_objlist_add(&db->cursor, &c->o);
		so_requestadd(e, task);
		return &c->o;
	}

	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQCURSOROPEN, &c->o, &db->o);
	sorequestarg *arg = &req.arg;
	arg->order = c->order;
	arg->vlsn_generate = 0;
	arg->vlsn = vlsn;
	arg->prefix = c->prefix;
	arg->prefixsize = c->prefixsize;
	so_query(&req);
	so_dbunref(c->db, 1);
	if (ssunlikely(req.rc == -1))
		goto error;
	so_dbbind(e);
	sr_objlist_add(&db->cursor, &c->o);
	return &c->o;
error:
	if (keyobj)
		sr_objdestroy(keyobj);
	if (c->cache)
		si_cachepool_push(c->cache);
	if (c)
		ss_free(&e->a_cursor, c);
	return NULL;
}
