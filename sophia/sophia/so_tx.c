
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

int so_txdbset(sodb *db, int async, uint8_t flags, va_list args)
{
	so *e = so_of(&db->o);

	/* validate request */
	sov *o = va_arg(args, sov*);
	if (ssunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return -1;
	}
	srobj *parent = o->parent;
	if (ssunlikely(parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return -1;
	}
	if (ssunlikely(! so_online(&db->status)))
		goto error;

	/* prepare object */
	svv *v = so_dbv(db, o, 0);
	if (ssunlikely(v == NULL))
		goto error;
	sr_objdestroy(&o->o);
	v->flags = flags;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);

	/* ensure quota */
	ss_quota(&e->quota, SS_QADD, sizeof(svv) + sv_size(&vp));

	/* asynchronous */
	if (async) {
		sorequest *task = so_requestnew(e, SO_REQDBSET, &db->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			return -1;
		}
		sorequestarg *arg = &task->arg;
		arg->vlsn_generate = 1;
		arg->v = vp;
		so_requestadd(e, task);
		return 0;
	}

	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQDBSET, &db->o, &db->o);
	sorequestarg *arg = &req.arg;
	arg->vlsn_generate = 1;
	arg->v = vp;
	so_query(&req);
	return req.rc;
error:
	sr_objdestroy(&o->o);
	return -1;
}

void *so_txdbget(sodb *db, int async, uint64_t vlsn, int vlsn_generate, va_list args)
{
	so *e = so_of(&db->o);

	/* validate request */
	sov *o = va_arg(args, sov*);
	if (ssunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	srobj *parent = o->parent;
	if (ssunlikely(parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return NULL;
	}
	if (ssunlikely(! so_online(&db->status)))
		goto error;

	/* prepare key object */
	svv *v = so_dbv(db, o, 1);
	if (ssunlikely(v == NULL))
		goto error;
	sr_objdestroy(&o->o);
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);

	/* asynchronous */
	if (async)
	{
		sorequest *task = so_requestnew(e, SO_REQDBGET, &db->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			return NULL;
		}
		sorequestarg *arg = &task->arg;
		arg->v = vp;
		arg->order = SS_EQ;
		arg->vlsn_generate = 1;
		so_requestadd(e, task);
		return &task->o;
	}

	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQDBGET, &db->o, &db->o);
	sorequestarg *arg = &req.arg;
	arg->v = vp;
	arg->order = SS_EQ;
	/* support (sync) snapshot read-view */
	arg->vlsn_generate = vlsn_generate;
	arg->vlsn = vlsn;
	so_query(&req);
	return req.result;
error:
	sr_objdestroy(&o->o);
	return NULL;
}

static int
so_txwrite(srobj *obj, uint8_t flags, va_list args)
{
	sotx *t = (sotx*)obj;
	so *e = so_of(obj);

	/* validate request */
	sov *o = va_arg(args, sov*);
	if (ssunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return -1;
	}
	srobj *parent = o->parent;
	if (parent == NULL || parent->id != SODB) {
		sr_error(&e->error, "%s", "bad object parent");
		return -1;
	}
	if (ssunlikely(t->t.s == SXPREPARE)) {
		sr_error(&e->error, "%s", "transaction is in 'prepare' state (read-only)");
		goto error;
	}

	/* validate database status */
	sodb *db = (sodb*)parent;
	int status = so_status(&db->status);
	switch (status) {
	case SO_ONLINE:
	case SO_RECOVER: break;
	case SO_SHUTDOWN:
		if (ssunlikely(! so_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	default: goto error;
	}

	/* prepare object */
	svv *v = so_dbv(db, o, 0);
	if (ssunlikely(v == NULL))
		goto error;
	v->flags = flags;
	v->log = o->log;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	sr_objdestroy(&o->o);

	/* ensure quota */
	ss_quota(&e->quota, SS_QADD, sizeof(svv) + sv_size(&vp));

	/* asynchronous */
	if (t->async) {
		sorequest *task = so_requestnew(e, SO_REQTXSET, &t->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			return -1;
		}
		sorequestarg *arg = &task->arg;
		arg->v = vp;
		so_requestadd(e, task);
		return 0;
	}

	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQTXSET, &t->o, &db->o);
	sorequestarg *arg = &req.arg;
	arg->v = vp;
	so_query(&req);
	return req.rc;
error:
	sr_objdestroy(&o->o);
	return -1;
}

static int
so_txset(srobj *o, va_list args)
{
	return so_txwrite(o, 0, args);
}

static int
so_txdelete(srobj *o, va_list args)
{
	return so_txwrite(o, SVDELETE, args);
}

static void*
so_txget(srobj *obj, va_list args)
{
	sotx *t = (sotx*)obj;
	so *e = so_of(obj);

	/* validate call */
	sov *o = va_arg(args, sov*);
	if (ssunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	srobj *parent = o->parent;
	if (parent == NULL || parent->id != SODB) {
		sr_error(&e->error, "%s", "bad object parent");
		return NULL;
	}

	/* validate database */
	sodb *db = (sodb*)parent;
	int status = so_status(&db->status);
	switch (status) {
	case SO_ONLINE:
	case SO_RECOVER:
		break;
	case SO_SHUTDOWN:
		if (ssunlikely(! so_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	default: goto error;
	}

	/* prepare key object */
	svv *v = so_dbv(db, o, 1);
	if (ssunlikely(v == NULL))
		goto error;
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	sr_objdestroy(&o->o);

	/* asynchronous */
	if (t->async) {
		sorequest *task = so_requestnew(e, SO_REQTXGET, &t->o, &db->o);
		if (ssunlikely(task == NULL)) {
			sv_vfree(db->r.a, v);
			return NULL;
		}
		sorequestarg *arg = &task->arg;
		arg->v = vp;
		arg->order = SS_EQ;
		arg->vlsn_generate = 0;
		so_requestadd(e, task);
		return &task->o;
	}

	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQTXGET, &t->o, &db->o);
	sorequestarg *arg = &req.arg;
	arg->v = vp;
	arg->order = SS_EQ;
	arg->vlsn_generate = 0;
	so_query(&req);
	return req.result;
error:
	sr_objdestroy(&o->o);
	return NULL;
}

void so_txend(sotx *t)
{
	so *e = so_of(&t->o);
	sx_gc(&t->t);
	so_dbunbind(e, t->t.id);
	sr_objlist_del(&e->tx, &t->o);
	ss_free(&e->a_tx, t);
}

static int
so_txrollback(srobj *o, va_list args ssunused)
{
	sotx *t = (sotx*)o;
	so *e = so_of(o);
	int shutdown = so_status(&e->status) == SO_SHUTDOWN;
	/* asynchronous */
	if (!shutdown && t->async) {
		sorequest *task = so_requestnew(e, SO_REQROLLBACK, &t->o, NULL);
		if (ssunlikely(task == NULL))
			return -1;
		so_requestadd(e, task);
		return 0;
	}
	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQROLLBACK, &t->o, NULL);
	so_query(&req);
	so_txend(t);
	return req.rc;
}

static int
so_txprepare(srobj *o, va_list args ssunused)
{
	sotx *t = (sotx*)o;
	so *e = so_of(o);
	int status = so_status(&e->status);
	if (ssunlikely(! so_statusactive_is(status)))
		return -1;
	/* asynchronous */
	if (t->async) {
		sorequest *task = so_requestnew(e, SO_REQPREPARE, &t->o, NULL);
		if (ssunlikely(task == NULL))
			return -1;
		task->arg.recover = (status == SO_RECOVER);
		so_requestadd(e, task);
		return 0;
	}
	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQPREPARE, &t->o, NULL);
	req.arg.recover = (status == SO_RECOVER);
	so_query(&req);
	if (ssunlikely(req.rc == 1))
		so_txend(t);
	return req.rc;
}

static int
so_txcommit(srobj *o, va_list args)
{
	sotx *t = (sotx*)o;
	so *e = so_of(o);
	int status = so_status(&e->status);
	if (ssunlikely(! so_statusactive_is(status)))
		return -1;

	/* prepare commit request */
	sorequest req;
	so_requestinit(e, &req, SO_REQCOMMIT, &t->o, NULL);
	sorequestarg *arg = &req.arg;
	arg->lsn = 0;
	if (status == SO_RECOVER || e->ctl.commit_lsn)
		arg->lsn = va_arg(args, uint64_t);
	if (ssunlikely(status == SO_RECOVER)) {
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
		sorequest *task = so_requestnew(e, SO_REQCOMMIT, &t->o, NULL);
		if (ssunlikely(task == NULL))
			return -1;
		*task = req;
		so_requestadd(e, task);
		return 0;
	}

	/* synchronous */
	so_query(&req);
	if (ssunlikely(req.rc == 2))
		return req.rc;
	so_txend(t);
	return req.rc;
}

static void*
so_txtype(srobj *o ssunused, va_list args ssunused) {
	return "transaction";
}

static srobjif sotxif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = so_txrollback,
	.error   = NULL,
	.set     = so_txset,
	.get     = so_txget,
	.del     = so_txdelete,
	.poll    = NULL,
	.drop    = so_txrollback,
	.begin   = NULL,
	.prepare = so_txprepare,
	.commit  = so_txcommit,
	.cursor  = NULL,
	.object  = NULL,
	.type    = so_txtype
};

srobj *so_txnew(so *e, int async)
{
	sotx *t = ss_malloc(&e->a_tx, sizeof(sotx));
	if (ssunlikely(t == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	sr_objinit(&t->o, SOTX, &sotxif, &e->o);
	sx_init(&e->xm, &t->t);
	t->async = async;
	/* asynchronous */
	if (async) {
		sorequest *task = so_requestnew(e, SO_REQBEGIN, &t->o, NULL);
		if (ssunlikely(task == NULL)) {
			ss_free(&e->a_tx, t);
			return NULL;
		}
		so_dbbind(e);
		sr_objlist_add(&e->tx, &t->o);
		so_requestadd(e, task);
		return &t->o;
	}
	/* synchronous */
	sorequest req;
	so_requestinit(e, &req, SO_REQBEGIN, &t->o, NULL);
	so_query(&req);
	so_dbbind(e);
	sr_objlist_add(&e->tx, &t->o);
	return &t->o;
}
