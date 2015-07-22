
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
#include <libse.h>

void
se_requestend(serequest *r)
{
	se *e = se_of(&r->o);
	/* free key */
	if (r->arg.v.v)
		sv_vfree(&e->a, r->arg.v.v);
	/* free request cache */
	if (sslikely(r->arg.cache))
		si_cachepool_push(r->arg.cache);
	/* unref db */
	if (sslikely(r->db))
		se_dbunref((sedb*)r->db, 1);
	if (ssunlikely(r->v))
		sv_vfree(&e->a, (svv*)r->v);
}

static int
se_requestdestroy(so *o)
{
	serequest *r = se_cast(o, serequest*, SEREQUEST);
	se *e = se_of(&r->o);
	se_requestend(r);
	/* free request result object */
	if (r->result)
		((so*)r->result)->i->destroy(r->result);
	/* gc used object */
	switch (r->op) {
	case SE_REQCURSOROPEN:
		if (ssunlikely(r->rc == -1))
			se_cursorend((secursor*)r->object);
		break;
	case SE_REQCURSORDESTROY:
		se_cursorend((secursor*)r->object);
		break;
	case SE_REQPREPARE:
	case SE_REQCOMMIT:
	case SE_REQROLLBACK:
		se_txend((setx*)r->object);
		break;
	default: break;
	}
	se_mark_destroyed(&r->o);
	ss_free(&e->a_req, r);
	return 0;
}

static inline char*
se_requestof(serequestop op)
{
	switch (op) {
	case SE_REQDBSET:
	case SE_REQTXSET:         return "set";
	case SE_REQDBGET:
	case SE_REQTXGET:         return "get";
	case SE_REQCURSOROPEN:    return "cursor";
	case SE_REQCURSORGET:     return "cursor_get";
	case SE_REQCURSORDESTROY: return "cursor_destroy";
	case SE_REQBEGIN:         return "begin";
	case SE_REQPREPARE:       return "prepare";
	case SE_REQCOMMIT:        return "commit";
	case SE_REQROLLBACK:      return "rollback";
	case SE_REQON_BACKUP:     return "backup";
	}
	return NULL;
}

static void*
se_requestget_object(so *o, char *path)
{
	serequest *r = se_cast(o, serequest*, SEREQUEST);
	if (strcmp(path, "result") == 0) {
		// lock?
		return r->result;
	} 
	return NULL;
}

static void*
se_requestget_string(so *o, char *path, int *size)
{
	serequest *r = se_cast(o, serequest*, SEREQUEST);
	if (strcmp(path, "type") == 0) {
		char *type = se_requestof(r->op);
		if (size)
			*size = strlen(type);
		return type;
	}
	return 0;
}

static int64_t
se_requestget_int(so *o, char *path)
{
	serequest *r = se_cast(o, serequest*, SEREQUEST);
	if (strcmp(path, "seq") == 0) {
		return r->id;
	} else
	if (strcmp(path, "status") == 0) {
		// lock?
		return r->rc;
	}
	return -1;
}

static soif serequestif =
{
	.open         = NULL,
	.destroy      = se_requestdestroy,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = se_requestget_object,
	.getstring    = se_requestget_string,
	.getint       = se_requestget_int,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = NULL,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

void se_requestinit(se *e, serequest *r, serequestop op, so *o, so *db)
{
	so_init(&r->o, &se_o[SEREQUEST], &serequestif, &e->o, &e->o);
	r->id = 0;
	r->op = op;
	r->object = o;
	r->db = db;
	if (r->db)
		se_dbref((sedb*)r->db, 1);
	memset(&r->arg, 0, sizeof(r->arg));
	r->result = NULL;
	r->v = NULL;
	r->rc = 0;
}

void se_requestadd(se *e, serequest *r)
{
	r->id = sr_seq(&e->seq, SR_RSNNEXT);
	ss_mutexlock(&e->reqlock);
	so_listadd(&e->req, &r->o);
	ss_condsignal(&e->reqcond);
	ss_mutexunlock(&e->reqlock);
}

void se_request_on_backup(se *e)
{
	if (e->meta.event_on_backup)
		ss_triggerrun(&e->meta.on_event);
}

void se_requestready(serequest *r)
{
	sedb *db = (sedb*)r->object;
	se *e = se_of(&db->o);
	ss_mutexlock(&e->reqlock);
	so_listdel(&e->reqactive, &r->o);
	so_listadd(&e->reqready, &r->o);
	ss_mutexunlock(&e->reqlock);
	ss_triggerrun(&e->meta.on_event);
}

serequest*
se_requestnew(se *e, serequestop op, so *object, so *db)
{
	serequest *r = ss_malloc(&e->a_req, sizeof(serequest));
	if (ssunlikely(r == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	se_requestinit(e, r, op, object, db);
	return r;
}

void se_requestwakeup(se *e)
{
	ss_mutexlock(&e->reqlock);
	ss_condsignal(&e->reqcond);
	ss_mutexunlock(&e->reqlock);
}

serequest*
se_requestdispatch(se *e, int block)
{
	ss_mutexlock(&e->reqlock);
	if (e->req.n == 0) {
		if (! block)
			goto empty;
		ss_condwait(&e->reqcond, &e->reqlock);
		if (e->req.n == 0)
			goto empty;
	}
	serequest *r = (serequest*)so_listfirst(&e->req);
	so_listdel(&e->req, &r->o);
	so_listadd(&e->reqactive, &r->o);
	ss_mutexunlock(&e->reqlock);
	return r;
empty:
	ss_mutexunlock(&e->reqlock);
	return NULL;
}

serequest*
se_requestdispatch_ready(se *e)
{
	ss_mutexlock(&e->reqlock);
	if (e->reqready.n == 0) {
		ss_mutexunlock(&e->reqlock);
		return NULL;
	}
	serequest *r = (serequest*)so_listfirst(&e->reqready);
	so_listdel(&e->reqready, &r->o);
	ss_mutexunlock(&e->reqlock);
	return r;
}

int se_requestqueue(se *e)
{
	ss_mutexlock(&e->reqlock);
	int n = e->req.n;
	ss_mutexunlock(&e->reqlock);
	return n;
}

int se_requestcount(se *e)
{
	ss_mutexlock(&e->reqlock);
	int n = e->req.n + e->reqactive.n + e->reqready.n;
	ss_mutexunlock(&e->reqlock);
	return n;
}

so *se_requestresult(serequest *r)
{
	if (r->op != SE_REQDBGET &&
	    r->op != SE_REQTXGET &&
	    r->op != SE_REQCURSORGET)
		return NULL;
	if (r->rc <= 0)
		return NULL;
	se *e = se_of(&r->o);
	sv result;
	sv_init(&result, &sv_vif, r->v, NULL);
	r->result = se_vnew(e, r->db, &result);
	if (sslikely(r->result))
		r->v = NULL;
	return r->result;
}
