
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

static inline int
so_querywrite(sorequest *r, svlog *log)
{
	sorequestarg *arg = &r->arg;
	so *e = so_of(r->object);
	/* set lsn */
	sl_prepare(&e->lp, log, arg->lsn);
	/* log write */
	if (! arg->recover) {
		sltx tl;
		sl_begin(&e->lp, &tl);
		int rc = sl_write(&tl, log);
		if (srunlikely(rc == -1)) {
			sl_rollback(&tl);
			return (r->rc = -1);
		}
		sl_commit(&tl);
	}
	/* commit */
	if (srlikely(arg->vlsn_generate))
		arg->vlsn = sx_vlsn(&e->xm);
	uint64_t now = sr_utime();
	svlogindex *i   = (svlogindex*)log->index.s;
	svlogindex *end = (svlogindex*)log->index.p;
	while (i < end) {
		sodb *db = i->ptr;
		sitx ti;
		si_begin(&ti, &db->r, &db->index, arg->vlsn, now, log, i);
		si_write(&ti, arg->recover);
		si_commit(&ti);
		i++;
	}
	return (r->rc = 0);
}

static inline int
so_queryread(sorequest *r)
{
	sorequestarg *arg = &r->arg;
	sodb *db = (sodb*)r->db;
	so *e = so_of(r->object);
	/* query */
	uint32_t keysize = sv_size(&arg->v);
	void *key = sv_pointer(&arg->v);
	if (srlikely(arg->vlsn_generate))
		arg->vlsn = sr_seq(db->r.seq, SR_LSN);
	sicache *cache = si_cachepool_pop(&e->cachepool);
	if (srunlikely(cache == NULL)) {
		sr_error(&e->error, "%s", "memory allocation error");
		return (r->rc = -1);
	}
	siquery q;
	si_queryopen(&q, &db->r, cache, &db->index,
	             arg->order,
	             arg->vlsn,
	             NULL, 0, key, keysize);
	int rc = si_query(&q);
	sv result = q.result;
	si_queryclose(&q);
	si_cachepool_push(cache);
	r->rc = rc;
	if (srunlikely(rc <= 0))
		return rc;
	/* result */
	soobj *ret = so_vdup(e, &db->o, &result);
	if (srunlikely(ret == NULL))
		sv_vfree(&e->a, (svv*)result.v);
	r->result = ret;
	return rc;
}

static inline int
so_querydb_set(sorequest *r)
{
	sorequestarg *arg = &r->arg;
	so *e = so_of(r->object);
	sodb *db = (sodb*)r->db;
	/* concurrency */
	sxstate s = sx_setstmt(&e->xm, &db->coindex, &arg->v);
	int rc = 1; /* rollback */
	switch (s) {
	case SXLOCK: rc = 2;
	case SXROLLBACK:
		sv_vfree(db->r.a, arg->v.v);
		arg->v.v = NULL;
		return (r->rc = rc);
	default: break;
	}
	svlogv v = {
		.id   = db->scheme.id,
		.next = UINT32_MAX,
		.v    = arg->v,
		.vgc  = NULL
	};
	svlog log;
	sv_loginit(&log);
	sv_logadd(&log, db->r.a, &v, db);
	so_querywrite(r, &log);
	if (srunlikely(r->rc == -1))
		sv_vfree(db->r.a, arg->v.v);
	arg->v.v = NULL;
	return r->rc;
}

static inline int
so_querydb_get(sorequest *r)
{
	so *e = so_of(r->object);
	sodb *db = (sodb*)r->db;
	/* register transaction statement */
	sx_getstmt(&e->xm, &db->coindex);
	so_queryread(r);
	sv_vfree(db->r.a, (svv*)r->arg.v.v);
	r->arg.v.v = NULL;
	return r->rc;
}

static inline int
so_querytx_set(sorequest *r)
{
	sorequestarg *arg = &r->arg;
	sodb *db = (sodb*)r->db;
	sotx *t = (sotx*)r->object;
	/* concurrent index only */
	r->rc = sx_set(&t->t, &db->coindex, (svv*)arg->v.v);
	if (srunlikely(r->rc == -1))
		sv_vfree(db->r.a, arg->v.v);
	arg->v.v = NULL;
	return r->rc;
}

static inline int
so_querytx_get(sorequest *r)
{
	sorequestarg *arg = &r->arg;
	sodb *db = (sodb*)r->db;
	sotx *t = (sotx*)r->object;
	so *e = so_of(r->object);
	/* derive vlsn */
	arg->vlsn = t->t.vlsn;
	arg->vlsn_generate = 0;
	/* concurrent */
	sv result;
	r->rc = sx_get(&t->t, &db->coindex, &arg->v, &result);
	switch (r->rc) {
	case  1:
		r->result = so_vdup(e, &db->o, &result);
		if (srunlikely(r->result == NULL)) {
			r->rc = -1;
			sv_vfree(&e->a, (svv*)result.v);
		}
		break;
	case  0:
		/* storage */
		so_queryread(r);
		break;
	}
	sv_vfree(db->r.a, (svv*)arg->v.v);
	arg->v.v = NULL;
	return r->rc;
}

static sxstate
so_queryprepare_trigger(sx *t, sv *v, void *arg0, void *arg1)
{
	sotx *te srunused = arg0;
	sodb *db = arg1;
	so *e = so_of(&db->o);
	uint64_t lsn = sr_seq(e->r.seq, SR_LSN);
	if (t->vlsn == lsn)
		return SXPREPARE;
	sicache *cache = si_cachepool_pop(&e->cachepool);
	if (srunlikely(cache == NULL)) {
		sr_error(&e->error, "%s", "memory allocation error");
		return SXROLLBACK;
	}
	siquery q;
	si_queryopen(&q, &db->r, cache, &db->index,
	             SR_UPDATE, t->vlsn,
	             NULL, 0,
	             sv_pointer(v), sv_size(v));
	int rc;
	rc = si_query(&q);
	if (rc == 1)
		sv_vfree(&e->a, (svv*)q.result.v);
	si_queryclose(&q);
	si_cachepool_push(cache);
	if (srunlikely(rc))
		return SXROLLBACK;
	return SXPREPARE;
}


static inline int
so_querybegin(sorequest *r)
{
	sotx *t = (sotx*)r->object;
	so *e = so_of(&t->o);
	assert(t->t.s == SXUNDEF);
	sx_begin(&e->xm, &t->t, 0);
	return 0;
}

static inline int
so_queryprepare(sorequest *r)
{
	sotx *t = (sotx*)r->object;
	if (srunlikely(t->t.s == SXPREPARE))
		return 0;
	/* resolve conflicts */
	sxpreparef prepare_trigger = so_queryprepare_trigger;
	if (r->arg.recover)
		prepare_trigger = NULL;
	sxstate s = sx_prepare(&t->t, prepare_trigger, t);
	if (s == SXLOCK)
		return (r->rc = 2);
	if (s == SXROLLBACK) {
		sx_rollback(&t->t);
		return (r->rc = 1);
	}
	assert(s == SXPREPARE);
	return 0;
}

static inline int
so_querycommit(sorequest *r)
{
	sotx *t = (sotx*)r->object;
	so *e = so_of(&t->o);
	/* prepare transaction for commit */
	if (srunlikely(! sv_logcount(&t->t.log))) {
		sx_prepare(&t->t, NULL, NULL);
		sx_commit(&t->t);
		return (r->rc = 0);
	}
	int rc;
	if (srlikely(t->t.s == SXREADY || t->t.s == SXLOCK))
	{
		sorequest req;
		so_requestinit(e, &req, SO_REQPREPARE, &t->o, NULL);
		req.arg.recover = r->arg.recover;
		rc = so_queryprepare(&req);
		if (srunlikely(rc != 0))
			return (r->rc = rc);
	}
	assert(t->t.s == SXPREPARE);
	sx_commit(&t->t);
	/* commit */
	return (r->rc = so_querywrite(r, &t->t.log));
}

static inline int
so_queryrollback(sorequest *r)
{
	sotx *t = (sotx*)r->object;
	sx_rollback(&t->t);
	return (r->rc = 0);
}

int so_query(sorequest *r)
{
	switch (r->op) {
	case SO_REQDBSET:    return so_querydb_set(r);
	case SO_REQDBGET:    return so_querydb_get(r);
	case SO_REQTXSET:    return so_querytx_set(r);
	case SO_REQTXGET:    return so_querytx_get(r);
	case SO_REQBEGIN:    return so_querybegin(r);
	case SO_REQPREPARE:  return so_queryprepare(r);
	case SO_REQCOMMIT:   return so_querycommit(r);
	case SO_REQROLLBACK: return so_queryrollback(r);
	default: assert(0);
	}
	return 0;
}
