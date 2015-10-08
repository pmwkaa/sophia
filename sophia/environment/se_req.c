
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
se_reqend(sereq *r)
{
	se *e = se_of(r->object);
	/* free key, prefix, update and a pending result */
	if (r->arg.v.v)
		sv_vfree(&e->r, r->arg.v.v);
	if (r->arg.vprefix.v)
		sv_vfree(&e->r, r->arg.vprefix.v);
	if (r->arg.vup.v)
		sv_vfree(&e->r, r->arg.vup.v);
	if (ssunlikely(r->v))
		sv_vfree(&e->r, (svv*)r->v);
	/* free read cache */
	if (sslikely(r->arg.cachegc && r->arg.cache))
		si_cachepool_push(r->arg.cache);
	/* unref db */
	if (sslikely(r->db))
		se_dbunref((sedb*)r->db, 1);
}

static int
se_reqdestroy(so *o)
{
	sereq *r = se_cast(o, sereq*, SEREQ);
	se *e = se_of(r->object);
	se_reqend(r);
	ss_free(&e->a_req, r);
	return 0;
}

static soif sereqif =
{
	.open         = NULL,
	.destroy      = se_reqdestroy,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
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

void se_reqinit(se *e, sereq *r, sereqop op, so *o, so *db)
{
	so_init(&r->o, &se_o[SEREQ], &sereqif, &e->o, &e->o);
	r->id = sr_seq(&e->seq, SR_RSNNEXT);
	r->op = op;
	r->object = o;
	r->db = db;
	if (db) {
		se_dbref((sedb*)db, 1);
	}
	memset(&r->arg, 0, sizeof(r->arg));
	r->v = NULL;
	r->rc = 0;
}

char *se_reqof(sereqop o)
{
	switch (o) {
	case SE_REQREAD:      return "on_read";
	case SE_REQWRITE:     return "on_write";
	case SE_REQON_BACKUP: return "on_backup";
	default: assert(0);
	}
	return NULL;
}

static void
se_reqadd(se *e, sereq *r)
{
	ss_mutexlock(&e->reqlock);
	so_listadd(&e->req, &r->o);
	ss_condsignal(&e->reqcond);
	ss_mutexunlock(&e->reqlock);
}

void se_reqonbackup(se *e)
{
	if (e->meta.event_on_backup)
		ss_triggerrun(&e->meta.on_event);
}

void se_reqready(sereq *r)
{
	sedb *db = (sedb*)r->object;
	se *e = se_of(&db->o);
	ss_mutexlock(&e->reqlock);
	so_listdel(&e->reqactive, &r->o);
	so_listadd(&e->reqready, &r->o);
	ss_mutexunlock(&e->reqlock);
	ss_triggerrun(&e->meta.on_event);
}

sereq*
se_reqnew(se *e, sereq *src, int add)
{
	sereq *r = ss_malloc(&e->a_req, sizeof(sereq));
	if (ssunlikely(r == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memcpy(r, src, sizeof(*r));
	if (add) {
		se_reqadd(e, r);
	}
	return r;
}

void se_reqwakeup(se *e)
{
	ss_mutexlock(&e->reqlock);
	ss_condsignal(&e->reqcond);
	ss_mutexunlock(&e->reqlock);
}

sereq*
se_reqdispatch(se *e, int block)
{
	ss_mutexlock(&e->reqlock);
	if (e->req.n == 0) {
		if (! block)
			goto empty;
		ss_condwait(&e->reqcond, &e->reqlock);
		if (e->req.n == 0)
			goto empty;
	}
	sereq *r = (sereq*)so_listfirst(&e->req);
	so_listdel(&e->req, &r->o);
	so_listadd(&e->reqactive, &r->o);
	ss_mutexunlock(&e->reqlock);
	return r;
empty:
	ss_mutexunlock(&e->reqlock);
	return NULL;
}

sereq*
se_reqdispatch_ready(se *e)
{
	ss_mutexlock(&e->reqlock);
	if (e->reqready.n == 0) {
		ss_mutexunlock(&e->reqlock);
		return NULL;
	}
	sereq *r = (sereq*)so_listfirst(&e->reqready);
	so_listdel(&e->reqready, &r->o);
	ss_mutexunlock(&e->reqlock);
	return r;
}

int se_reqqueue(se *e)
{
	ss_mutexlock(&e->reqlock);
	int n = e->req.n;
	ss_mutexunlock(&e->reqlock);
	return n;
}

int se_reqcount(se *e)
{
	ss_mutexlock(&e->reqlock);
	int n = e->req.n + e->reqactive.n + e->reqready.n;
	ss_mutexunlock(&e->reqlock);
	return n;
}

so *se_reqresult(sereq *r, int async)
{
	se *e = se_of(&r->o);
	sv result;
	sv_init(&result, &sv_vif, r->v, NULL);
	sev *v = (sev*)se_vnew(e, r->db, &result, async);
	if (ssunlikely(v == NULL))
		return NULL;
	r->v = NULL;
	v->async_operation = r->op;
	v->async_status    = r->rc;
	v->async_seq       = r->id;
	v->async_arg       = r->arg.arg;
	v->cache_only      = r->arg.cache_only;
	/* propagate current object settings to
	 * the result one */
	v->orderset = 1;
	v->order = r->arg.order;
	if (v->order == SS_GTE)
		v->order = SS_GT;
	else
	if (v->order == SS_LTE)
		v->order = SS_LT;
	/* reuse prefix object */
	v->vprefix.v = r->arg.vprefix.v;
	if (v->vprefix.v) {
		r->arg.vprefix.v = NULL;
		void *vptr = sv_vpointer(v->vprefix.v);
		v->prefix = sf_key(vptr, 0);
		v->prefixsize = sf_keysize(vptr, 0);
	}
	return &v->o;
}
