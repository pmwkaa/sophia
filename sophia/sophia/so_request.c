
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
so_requestdestroy(srobj *obj, va_list args ssunused)
{
	sorequest *r = (sorequest*)obj;
	so *e = so_of(&r->o);
	if (r->result)
		sr_objdestroy((srobj*)r->result);
	if (r->arg.v.v)
		sv_vfree(&e->a, r->arg.v.v);
	switch (r->op) {
	case SO_REQCURSOROPEN:
		if (ssunlikely(r->rc == -1))
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
	ss_free(&e->a_req, r);
	return 0;
}

static void*
so_requesttype(srobj *o ssunused, va_list args ssunused) {
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
so_requestget(srobj *obj, va_list args)
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

static srobjif sorequestif =
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

void so_requestinit(so *e, sorequest *r, sorequestop op, srobj *object, srobj *db)
{
	sr_objinit(&r->o, SOREQUEST, &sorequestif, &e->o);
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
	ss_spinlock(&e->reqlock);
	sr_objlist_add(&e->reqready, &r->o);
	ss_spinunlock(&e->reqlock);
}

void so_requestadd(so *e, sorequest *r)
{
	r->id = sr_seq(&e->seq, SR_RSNNEXT);
	ss_spinlock(&e->reqlock);
	sr_objlist_add(&e->req, &r->o);
	ss_spinunlock(&e->reqlock);
}

void so_request_on_backup(so *e)
{
	if (e->ctl.event_on_backup)
		ss_triggerrun(&e->ctl.on_event);
}

void so_requestready(sorequest *r)
{
	sodb *db = (sodb*)r->object;
	so *e = so_of(&db->o);
	ss_triggerrun(&e->ctl.on_event);
	/* ready for polling */
	so_requestadd_ready(e, r);
}

sorequest*
so_requestnew(so *e, sorequestop op, srobj *object, srobj *db)
{
	sorequest *r = ss_malloc(&e->a_req, sizeof(sorequest));
	if (ssunlikely(r == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	so_requestinit(e, r, op, object, db);
	return r;
}

sorequest*
so_requestdispatch(so *e)
{
	ss_spinlock(&e->reqlock);
	if (e->req.n == 0) {
		ss_spinunlock(&e->reqlock);
		return NULL;
	}
	sorequest *req = (sorequest*)sr_objlist_first(&e->req);
	sr_objlist_del(&e->req, &req->o);
	ss_spinunlock(&e->reqlock);
	return req;
}

sorequest*
so_requestdispatch_ready(so *e)
{
	ss_spinlock(&e->reqlock);
	if (e->reqready.n == 0) {
		ss_spinunlock(&e->reqlock);
		return NULL;
	}
	sorequest *req = (sorequest*)sr_objlist_first(&e->reqready);
	sr_objlist_del(&e->reqready, &req->o);
	ss_spinunlock(&e->reqlock);
	return req;
}

int so_requestcount(so *e)
{
	ss_spinlock(&e->reqlock);
	int n = e->req.n;
	ss_spinunlock(&e->reqlock);
	return n;
}
