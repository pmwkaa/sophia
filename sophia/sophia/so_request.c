
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

void
so_requestend(sorequest *r)
{
	so *e = so_of(&r->o);
	/* free key */
	if (r->arg.v.v)
		sv_vfree(&e->a, r->arg.v.v);
	/* free request cache */
	if (sslikely(r->arg.cache))
		si_cachepool_push(r->arg.cache);
	/* unref db */
	if (sslikely(r->db))
		so_dbunref((sodb*)r->db, 1);
	if (ssunlikely(r->v))
		sv_vfree(&e->a, (svv*)r->v);
}

static int
so_requestdestroy(srobj *obj, va_list args ssunused)
{
	sorequest *r = (sorequest*)obj;
	so *e = so_of(&r->o);
	so_requestend(r);
	/* free request result object */
	if (r->result)
		sr_objdestroy((srobj*)r->result);
	/* gc used object */
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
	if (r->db)
		so_dbref((sodb*)r->db, 1);
	memset(&r->arg, 0, sizeof(r->arg));
	r->result = NULL;
	r->v = NULL;
	r->rc = 0;
}

void so_requestwakeup(so *e)
{
	ss_mutexlock(&e->reqlock);
	ss_condsignal(&e->reqcond);
	ss_mutexunlock(&e->reqlock);
}

void so_requestadd(so *e, sorequest *r)
{
	r->id = sr_seq(&e->seq, SR_RSNNEXT);
	ss_mutexlock(&e->reqlock);
	sr_objlist_add(&e->req, &r->o);
	ss_condsignal(&e->reqcond);
	ss_mutexunlock(&e->reqlock);
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
	ss_mutexlock(&e->reqlock);
	sr_objlist_del(&e->reqactive, &r->o);
	sr_objlist_add(&e->reqready, &r->o);
	ss_mutexunlock(&e->reqlock);
	ss_triggerrun(&e->ctl.on_event);
}

sorequest*
so_requestnew(so *e, sorequestop op, srobj *object, srobj *db)
{
	sorequest *r = ss_malloc(&e->a_req, sizeof(sorequest));
	if (ssunlikely(r == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_requestinit(e, r, op, object, db);
	return r;
}

sorequest*
so_requestdispatch(so *e, int block)
{
	ss_mutexlock(&e->reqlock);
	if (e->req.n == 0) {
		if (! block)
			goto empty;
		ss_condwait(&e->reqcond, &e->reqlock);
		if (ssunlikely(e->req.n == 0))
			goto empty;
	}
	sorequest *r = (sorequest*)sr_objlist_first(&e->req);
	sr_objlist_del(&e->req, &r->o);
	sr_objlist_add(&e->reqactive, &r->o);
	ss_mutexunlock(&e->reqlock);
	return r;
empty:
	ss_mutexunlock(&e->reqlock);
	return NULL;
}

sorequest*
so_requestdispatch_ready(so *e)
{
	ss_mutexlock(&e->reqlock);
	if (e->reqready.n == 0) {
		ss_mutexunlock(&e->reqlock);
		return NULL;
	}
	sorequest *r = (sorequest*)sr_objlist_first(&e->reqready);
	sr_objlist_del(&e->reqready, &r->o);
	ss_mutexunlock(&e->reqlock);
	return r;
}

int so_requestqueue(so *e)
{
	ss_mutexlock(&e->reqlock);
	int n = e->req.n;
	ss_mutexunlock(&e->reqlock);
	return n;
}

int so_requestcount(so *e)
{
	ss_mutexlock(&e->reqlock);
	int n = e->req.n + e->reqactive.n + e->reqready.n;
	ss_mutexunlock(&e->reqlock);
	return n;
}

srobj*
so_requestresult(sorequest *r)
{
	if (r->op != SO_REQDBGET &&
	    r->op != SO_REQTXGET &&
	    r->op != SO_REQCURSORGET)
		return NULL;
	if (r->rc <= 0)
		return NULL;
	so *e = so_of(&r->o);
	sv result;
	sv_init(&result, &sv_vif, r->v, NULL);
	r->result = so_vdup(e, r->db, &result);
	if (sslikely(r->result))
		r->v = NULL;
	return r->result;
}
