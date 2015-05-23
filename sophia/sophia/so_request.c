
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

static int
so_requestdestroy(soobj *obj, va_list args srunused)
{
	sorequest *r = (sorequest*)obj;
	so *e = so_of(&r->o);
	if (r->result)
		so_objdestroy((soobj*)r->result);
	if (r->arg.v.v)
		sv_vfree(&e->a, r->arg.v.v);
	switch (r->op) {
	case SO_REQCURSOROPEN:
		if (srunlikely(r->rc == -1))
			so_cursorend((socursor*)r->object);
		break;
	case SO_REQCURSORDESTROY:
		so_cursorend((socursor*)r->object);
		break;
	case SO_REQPREPARE:
	case SO_REQCOMMIT:
	case SO_REQROLLBACK:
		so_txend((sotx*)r->object);
		break;
	default: break;
	}
	sr_free(&e->a_req, r);
	return 0;
}

static void*
so_requesttype(soobj *o srunused, va_list args srunused) {
	return "request";
}

static inline char*
so_requestof(sorequestop op)
{
	switch (op) {
	case SO_REQDBSET:
	case SO_REQTXSET:         return "set";
	case SO_REQDBGET:
	case SO_REQTXGET:         return "get";
	case SO_REQCURSOROPEN:    return "cursor";
	case SO_REQCURSORGET:     return "cursor_get";
	case SO_REQCURSORDESTROY: return "cursor_destroy";
	case SO_REQBEGIN:         return "begin";
	case SO_REQPREPARE:       return "prepare";
	case SO_REQCOMMIT:        return "commit";
	case SO_REQROLLBACK:      return "rollback";
	case SO_REQON_BACKUP:     return "backup";
	}
	return NULL;
}

static void*
so_requestget(soobj *obj, va_list args)
{
	sorequest *r = (sorequest*)obj;
	char *name = va_arg(args, char*);
	if (strcmp(name, "seq") == 0) {
		return &r->id;
	} else
	if (strcmp(name, "type") == 0) {
		return so_requestof(r->op);
	} else
	if (strcmp(name, "status") == 0) {
		// lock?
		return &r->rc;
	} else
	if (strcmp(name, "result") == 0) {
		// lock?
		return r->result;
	} 
	return NULL;
}

static soobjif sorequestif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = so_requestdestroy,
	.error   = NULL,
	.set     = NULL,
	.del     = NULL,
	.get     = so_requestget,
	.poll    = NULL,
	.drop    = NULL,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = NULL,
	.type    = so_requesttype
};

void so_requestinit(so *e, sorequest *r, sorequestop op, soobj *object, soobj *db)
{
	so_objinit(&r->o, SOREQUEST, &sorequestif, &e->o);
	r->id = 0;
	r->op = op;
	r->object = object;
	r->db = db;
	memset(&r->arg, 0, sizeof(r->arg));
	r->result = NULL;
	r->rc = 0;
}

static inline void
so_requestadd_ready(so *e, sorequest *r)
{
	sr_spinlock(&e->reqlock);
	so_objindex_register(&e->reqready, &r->o);
	sr_spinunlock(&e->reqlock);
}

void so_requestadd(so *e, sorequest *r)
{
	r->id = sr_seq(&e->seq, SR_RSNNEXT);
	sr_spinlock(&e->reqlock);
	so_objindex_register(&e->req, &r->o);
	sr_spinunlock(&e->reqlock);
}

void so_request_on_backup(so *e)
{
	if (e->ctl.event_on_backup)
		sr_triggerrun(&e->ctl.on_event);
}

void so_requestready(sorequest *r)
{
	sodb *db = (sodb*)r->object;
	so *e = so_of(&db->o);
	sr_triggerrun(&e->ctl.on_event);
	/* ready for polling */
	so_requestadd_ready(e, r);
}

sorequest*
so_requestnew(so *e, sorequestop op, soobj *object, soobj *db)
{
	sorequest *r = sr_malloc(&e->a_req, sizeof(sorequest));
	if (srunlikely(r == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	so_requestinit(e, r, op, object, db);
	return r;
}

sorequest*
so_requestdispatch(so *e)
{
	sr_spinlock(&e->reqlock);
	if (e->req.n == 0) {
		sr_spinunlock(&e->reqlock);
		return NULL;
	}
	sorequest *req = (sorequest*)so_objindex_first(&e->req);
	so_objindex_unregister(&e->req, &req->o);
	sr_spinunlock(&e->reqlock);
	return req;
}

sorequest*
so_requestdispatch_ready(so *e)
{
	sr_spinlock(&e->reqlock);
	if (e->reqready.n == 0) {
		sr_spinunlock(&e->reqlock);
		return NULL;
	}
	sorequest *req = (sorequest*)so_objindex_first(&e->reqready);
	so_objindex_unregister(&e->reqready, &req->o);
	sr_spinunlock(&e->reqlock);
	return req;
}

int so_requestcount(so *e)
{
	sr_spinlock(&e->reqlock);
	int n = e->req.n;
	sr_spinunlock(&e->reqlock);
	return n;
}
