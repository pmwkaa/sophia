
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

int se_txdbwrite(sedb *db, sev *o, int async, uint8_t flags)
{
	se *e = se_of(&db->o);
	/* validate request */
	if (ssunlikely(o->o.parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return -1;
	}
	if (ssunlikely(! se_online(&db->status)))
		goto error;
	if (flags == SVUPDATE && !sf_updatehas(&db->scheme.fmt_update))
		flags = 0;

	/* prepare object */
	svv *v = se_dbv(db, o, 0);
	if (ssunlikely(v == NULL))
		goto error;
	o->o.i->destroy(&o->o);
	v->flags = flags;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);

	/* ensure quota */
	ss_quota(&e->quota, SS_QADD, sizeof(svv) + sv_size(&vp));

	/* asynchronous */
	if (async) {
		serequest *task = se_requestnew(e, SE_REQDBSET, &db->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			return -1;
		}
		serequestarg *arg = &task->arg;
		arg->vlsn_generate = 1;
		arg->v = vp;
		se_requestadd(e, task);
		return 0;
	}

	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQDBSET, &db->o, &db->o);
	serequestarg *arg = &req.arg;
	arg->vlsn_generate = 1;
	arg->v = vp;
	se_query(&req);
	se_requestend(&req);
	return req.rc;
error:
	o->o.i->destroy(&o->o);
	return -1;
}

void *se_txdbget(sedb *db, sev *o, int async, uint64_t vlsn, int vlsn_generate)
{
	se *e = se_of(&db->o);
	/* validate request */
	if (ssunlikely(o->o.parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return NULL;
	}
	if (ssunlikely(! se_online(&db->status)))
		goto error;

	/* prepare key object */
	svv *v = se_dbv(db, o, 1);
	if (ssunlikely(v == NULL))
		goto error;
	o->o.i->destroy(&o->o);
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);

	sicache *cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(cache == NULL)) {
		sr_oom(&e->error);
		sv_vfree(db->r.a, v);
		return NULL;
	}

	/* asynchronous */
	if (async)
	{
		serequest *task = se_requestnew(e, SE_REQDBGET, &db->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			si_cachepool_push(cache);
			return NULL;
		}
		serequestarg *arg = &task->arg;
		arg->v = vp;
		arg->cache = cache;
		arg->order = SS_EQ;
		arg->vlsn_generate = 1;
		se_requestadd(e, task);
		return &task->o;
	}

	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQDBGET, &db->o, &db->o);
	serequestarg *arg = &req.arg;
	arg->v = vp;
	arg->cache = cache;
	arg->order = SS_EQ;
	arg->vlsn_generate = vlsn_generate;
	arg->vlsn = vlsn;
	int rc = se_query(&req);
	if (rc == 1) {
		se_requestresult(&req);
		se_requestend(&req);
		return req.result;
	}
	se_requestend(&req);
	return NULL;
error:
	o->o.i->destroy(&o->o);
	return NULL;
}

static inline int
se_txwrite(setx *t, sev *o, uint8_t flags)
{
	se *e = se_of(&t->o);
	sedb *db = se_cast(o->o.parent, sedb*, SEDB);
	/* validate request */
	if (ssunlikely(t->t.s == SXPREPARE)) {
		sr_error(&e->error, "%s", "transaction is in 'prepare' state (read-only)");
		goto error;
	}
	/* validate database status */
	int status = se_status(&db->status);
	switch (status) {
	case SE_ONLINE:
	case SE_RECOVER: break;
	case SE_SHUTDOWN:
		if (ssunlikely(! se_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	default: goto error;
	}
	if (flags == SVUPDATE && !sf_updatehas(&db->scheme.fmt_update))
		flags = 0;
	/* prepare object */
	svv *v = se_dbv(db, o, 0);
	if (ssunlikely(v == NULL))
		goto error;
	v->flags = flags;
	v->log = o->log;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	o->o.i->destroy(&o->o);

	/* ensure quota */
	ss_quota(&e->quota, SS_QADD, sizeof(svv) + sv_size(&vp));

	/* asynchronous */
	if (t->async) {
		serequest *task = se_requestnew(e, SE_REQTXSET, &t->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			return -1;
		}
		serequestarg *arg = &task->arg;
		arg->v = vp;
		se_requestadd(e, task);
		return 0;
	}

	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQTXSET, &t->o, &db->o);
	serequestarg *arg = &req.arg;
	arg->v = vp;
	se_query(&req);
	se_requestend(&req);
	return req.rc;
error:
	o->o.i->destroy(&o->o);
	return -1;
}

static int
se_txset(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	return se_txwrite(t, key, 0);
}

static int
se_txupdate(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	return se_txwrite(t, key, SVUPDATE);
}

static int
se_txdelete(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	return se_txwrite(t, key, SVDELETE);
}

static inline void*
se_txread(setx *t, sev *o)
{
	se *e = se_of(&t->o);
	sedb *db = se_cast(o->o.parent, sedb*, SEDB);

	/* validate database */
	int status = se_status(&db->status);
	switch (status) {
	case SE_ONLINE:
	case SE_RECOVER:
		break;
	case SE_SHUTDOWN:
		if (ssunlikely(! se_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	default: goto error;
	}

	/* prepare key object */
	svv *v = se_dbv(db, o, 1);
	if (ssunlikely(v == NULL))
		goto error;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	o->o.i->destroy(&o->o);

	sicache *cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(cache == NULL)) {
		sr_oom(&e->error);
		sv_vfree(db->r.a, v);
		return NULL;
	}

	/* asynchronous */
	if (t->async) {
		serequest *task = se_requestnew(e, SE_REQTXGET, &t->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			si_cachepool_push(cache);
			return NULL;
		}
		serequestarg *arg = &task->arg;
		arg->v = vp;
		arg->cache = cache;
		arg->order = SS_EQ;
		arg->vlsn_generate = 0;
		se_requestadd(e, task);
		return &task->o;
	}

	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQTXGET, &t->o, &db->o);
	serequestarg *arg = &req.arg;
	arg->v = vp;
	arg->cache = cache;
	arg->order = SS_EQ;
	arg->vlsn_generate = 0;
	int rc = se_query(&req);
	if (rc == 1) {
		se_requestresult(&req);
		se_requestend(&req);
		return req.result;
	}
	se_requestend(&req);
	return NULL;
error:
	o->o.i->destroy(&o->o);
	return NULL;
}

static void*
se_txget(so *o, so *v)
{
	setx  *t = se_cast(o, setx*, SETX);
	sev *key = se_cast(v, sev*, SEV);
	return se_txread(t, key);
}

void se_txend(setx *t)
{
	se *e = se_of(&t->o);
	sx_gc(&t->t);
	se_dbunbind(e, t->t.id);
	so_listdel(&e->tx, &t->o);
	ss_free(&e->a_tx, t);
}

static int
se_txrollback(so *o)
{
	setx *t = se_cast(o, setx*, SETX);
	se *e = se_of(o);
	int shutdown = se_status(&e->status) == SE_SHUTDOWN;
	/* asynchronous */
	if (!shutdown && t->async) {
		serequest *task = se_requestnew(e, SE_REQROLLBACK, &t->o, NULL);
		if (ssunlikely(task == NULL))
			return -1;
		se_requestadd(e, task);
		return 0;
	}
	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQROLLBACK, &t->o, NULL);
	se_query(&req);
	se_txend(t);
	return req.rc;
}

static int
se_txprepare(so *o)
{
	setx *t = se_cast(o, setx*, SETX);
	se *e = se_of(o);
	int status = se_status(&e->status);
	if (ssunlikely(! se_statusactive_is(status)))
		return -1;

	sicache *cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(cache == NULL))
		return sr_oom(&e->error);

	/* asynchronous */
	if (t->async) {
		serequest *task = se_requestnew(e, SE_REQPREPARE, &t->o, NULL);
		if (ssunlikely(task == NULL)) {
			si_cachepool_push(cache);
			return -1;
		}
		serequestarg *arg = &task->arg;
		arg->recover = (status == SE_RECOVER);
		arg->cache = cache;
		se_requestadd(e, task);
		return 0;
	}

	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQPREPARE, &t->o, NULL);
	serequestarg *arg = &req.arg;
	arg->recover = (status == SE_RECOVER);
	arg->cache = cache;
	se_query(&req);
	se_requestend(&req);
	if (ssunlikely(req.rc == 1))
		se_txend(t);
	return req.rc;
}

static int
se_txcommit(so *o)
{
	setx *t = se_cast(o, setx*, SETX);
	se *e = se_of(o);
	int status = se_status(&e->status);
	if (ssunlikely(! se_statusactive_is(status)))
		return -1;

	sicache *cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(cache == NULL))
		return sr_oom(&e->error);

	/* prepare commit request */
	serequest req;
	se_requestinit(e, &req, SE_REQCOMMIT, &t->o, NULL);
	serequestarg *arg = &req.arg;
	arg->cache = cache;
	arg->lsn = 0;
	if (status == SE_RECOVER || e->meta.commit_lsn)
		arg->lsn = t->lsn;
	if (ssunlikely(status == SE_RECOVER)) {
		assert(t->async == 0);
		arg->recover = 1;
		arg->vlsn_generate = 0;
		arg->vlsn = sr_seq(e->r.seq, SR_LSN);
	} else {
		arg->vlsn_generate = 1;
		arg->vlsn = 0;
	}

	/* asynchronous */
	if (t->async) {
		serequest *task = se_requestnew(e, SE_REQCOMMIT, &t->o, NULL);
		if (ssunlikely(task == NULL))
			return -1;
		*task = req;
		se_requestadd(e, task);
		return 0;
	}

	/* synchronous */
	se_query(&req);
	se_requestend(&req);
	if (ssunlikely(req.rc != 2))
		se_txend(t);
	return req.rc;
}

static int
se_txset_int(so *o, char *path, int64_t v)
{
	setx *t = se_cast(o, setx*, SETX);
	if (strcmp(path, "lsn") == 0) {
		t->lsn = v;
		return 0;
	}
	return -1;
}

static soif setxif =
{
	.open         = NULL,
	.destroy      = se_txrollback,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = se_txset_int,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = se_txset,
	.update       = se_txupdate,
	.del          = se_txdelete,
	.get          = se_txget,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = se_txprepare,
	.commit       = se_txcommit,
	.cursor       = NULL,
};

so *se_txnew(se *e, int async)
{
	setx *t = ss_malloc(&e->a_tx, sizeof(setx));
	if (ssunlikely(t == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&t->o, &se_o[SETX], &setxif, &e->o, &e->o);
	sx_init(&e->xm, &t->t);
	t->lsn   = 0;
	t->async = async;
	/* asynchronous */
	if (async) {
		serequest *task = se_requestnew(e, SE_REQBEGIN, &t->o, NULL);
		if (ssunlikely(task == NULL)) {
			ss_free(&e->a_tx, t);
			return NULL;
		}
		se_dbbind(e);
		so_listadd(&e->tx, &t->o);
		se_requestadd(e, task);
		return &t->o;
	}
	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQBEGIN, &t->o, NULL);
	se_query(&req);
	se_dbbind(e);
	so_listadd(&e->tx, &t->o);
	return &t->o;
}
