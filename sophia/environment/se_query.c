
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

static inline int
se_querywrite(serequest *r, svlog *log)
{
	serequestarg *arg = &r->arg;
	se *e = se_of(r->object);
	/* set lsn */
	sl_prepare(&e->lp, log, arg->lsn);
	/* log write */
	if (! arg->recover) {
		sltx tl;
		sl_begin(&e->lp, &tl);
		int rc = sl_write(&tl, log);
		if (ssunlikely(rc == -1)) {
			sl_rollback(&tl);
			return (r->rc = -1);
		}
		sl_commit(&tl);
	}
	/* commit */
	if (sslikely(arg->vlsn_generate))
		arg->vlsn = sx_vlsn(&e->xm);
	uint64_t now = ss_utime();
	svlogindex *i   = (svlogindex*)log->index.s;
	svlogindex *end = (svlogindex*)log->index.p;
	while (i < end) {
		sedb *db = i->ptr;
		sitx ti;
		si_begin(&ti, &db->index, arg->vlsn, now, log, i);
		si_write(&ti, arg->recover);
		si_commit(&ti);
		i++;
	}
	return (r->rc = 0);
}

static inline int
se_queryread(serequest *r)
{
	serequestarg *arg = &r->arg;
	sedb *db = (sedb*)r->db;
	uint32_t keysize;
	void *key;
	if (sslikely(arg->v.v)) {
		keysize = sv_size(&arg->v);
		key = sv_pointer(&arg->v);
	} else {
		keysize = 0;
		key = NULL;
	}
	if (sslikely(arg->vlsn_generate))
		arg->vlsn = sr_seq(db->r.seq, SR_LSN);
	siquery q;
	si_queryopen(&q, arg->cache, &db->index,
	             arg->order,
	             arg->vlsn,
	             arg->prefix,
	             arg->prefixsize, key, keysize);
	if (arg->update)
		si_queryupdate(&q, arg->update_v);
	r->rc = si_query(&q);
	r->v = q.result.v;
	si_queryclose(&q);
	return r->rc;
}

static inline int
se_querydb_set(serequest *r)
{
	serequestarg *arg = &r->arg;
	se *e = se_of(r->object);
	sedb *db = (sedb*)r->db;
	/* concurrency */
	sxstate s = sx_setstmt(&e->xm, &db->coindex, &arg->v);
	int rc = 1; /* rollback */
	switch (s) {
	case SXLOCK: rc = 2;
	case SXROLLBACK:
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
	se_querywrite(r, &log);
	arg->v.v = NULL;
	return r->rc;
}

static inline int
se_querydb_get(serequest *r)
{
	serequestarg *arg = &r->arg;
	sedb *db = (sedb*)r->db;
	se *e = se_of(r->object);
	/* register transaction statement */
	sx_getstmt(&e->xm, &db->coindex);
	/* switch to curser iteration to support
	 * update operations */
	if (sf_updatehas(&db->scheme.fmt_update)) {
		arg->order    = SS_LTE;
		arg->update   = 1;
		arg->update_v = NULL;
	}
	se_queryread(r);
	return r->rc;
}

static inline int
se_querytx_set(serequest *r)
{
	serequestarg *arg = &r->arg;
	sedb *db = (sedb*)r->db;
	setx *t = (setx*)r->object;
	/* concurrent index only */
	r->rc = sx_set(&t->t, &db->coindex, (svv*)arg->v.v);
	if (sslikely(r->rc == 0))
		arg->v.v = NULL;
	return r->rc;
}

static inline int
se_querytx_get(serequest *r)
{
	serequestarg *arg = &r->arg;
	sedb *db = (sedb*)r->db;
	setx *t = (setx*)r->object;
	/* derive vlsn */
	arg->vlsn = t->t.vlsn;
	arg->vlsn_generate = 0;
	/* concurrent */
	sv result = { .v = NULL };
	r->rc = sx_get(&t->t, &db->coindex, &arg->v, &result);
	switch (r->rc) {
	case  1:
		if (! sv_is(&result, SVUPDATE)) {
			r->v = result.v;
			break;
		}
		arg->order    = SS_LTE;
		arg->update   = 1;
		arg->update_v = &result;
	case  0:
		/* storage */
		se_queryread(r);
		if (result.v)
			sv_vfree(db->r.a, result.v);
		break;
	}
	return r->rc;
}

static inline int
se_querycurser_get(serequest *r)
{
	secursor *c = (secursor*)r->object;
	se *e = se_of(r->object);
	if (ssunlikely(c->order == SS_STOP))
		return (r->rc = 0);
	if (ssunlikely(c->v.v == NULL))
		return (r->rc = 0);

	/* reuse first key */
	if (ssunlikely(c->ready))
	{
		r->v = sv_vdup(&e->a, &c->v);
		if (ssunlikely(r->v == NULL)) {
			sr_oom(&e->error);
			return -1;
		}
		c->ready = 0;
		return (r->rc = 1);
	}

	/* read next */
	serequestarg *arg = &r->arg;
	svv *v = NULL;
	arg->cache = c->cache;
	arg->v = c->v;
	se_queryread(r);
	arg->cache = NULL;
	arg->v.v = NULL;
	switch (r->rc) {
	case -1: return r->rc;
	case  1: {
		sv vp;
		sv_init(&vp, &sv_vif, r->v, NULL);
		v = sv_vdup(&e->a, &vp);
		if (ssunlikely(v == NULL)) {
			sr_oom(&e->error);
			return -1;
		}
		break;
	}
	}

	/* free previous */
	sv_vfree(c->db->r.a, c->v.v);
	sv_init(&c->v, &sv_vif, v, NULL);
	return r->rc;
}

static inline int
se_querycurser_open(serequest *r)
{
	serequestarg *arg = &r->arg;
	secursor *c = (secursor*)r->object;
	se *e = se_of(r->object);

	/* start curser transaction */
	sx_begin(&e->xm, &c->t, arg->vlsn);

	/* read */
	arg->cache = c->cache;
	arg->vlsn = c->t.vlsn;
	arg->v = c->seek;
	se_queryread(r);
	arg->cache = NULL;
	arg->v.v = NULL;
	switch (r->rc) {
	case  1:
		sv_init(&c->v, &sv_vif, r->v, NULL);
		r->v = NULL;
		c->ready = 1;
		break;
	case -1:
		sx_rollback(&c->t);
		return -1;
	}

	/* ensure correct iteration */
	ssorder next = SS_GTE;
	switch (c->order) {
	case SS_LT:
	case SS_LTE: next = SS_LT;
		break;
	case SS_GT:
	case SS_GTE: next = SS_GT;
		break;
	default: assert(0);
	}
	c->order = next;
	return r->rc;
}

static inline int
se_querycurser_destroy(serequest *r)
{
	secursor *c = (secursor*)r->object;
	sx_rollback(&c->t);
	return 0;
}

static inline int
se_querybegin(serequest *r)
{
	setx *t = (setx*)r->object;
	se *e = se_of(&t->o);
	assert(t->t.s == SXUNDEF);
	sx_begin(&e->xm, &t->t, 0);
	return 0;
}

static sxstate
se_queryprepare_trigger(sx *t, sv *v, void *arg0, void *arg1)
{
	sicache *cache = arg0;
	sedb *db = arg1;
	se *e = se_of(&db->o);
	uint64_t lsn = sr_seq(e->r.seq, SR_LSN);
	if (t->vlsn == lsn)
		return SXPREPARE;
	siquery q;
	si_queryopen(&q, cache, &db->index,
	             SS_HAS, t->vlsn,
	             NULL, 0,
	             sv_pointer(v), sv_size(v));
	int rc;
	rc = si_query(&q);
	if (rc == 1)
		sv_vfree(&e->a, (svv*)q.result.v);
	si_queryclose(&q);
	if (ssunlikely(rc))
		return SXROLLBACK;
	return SXPREPARE;
}

static inline int
se_queryprepare(serequest *r)
{
	setx *t = (setx*)r->object;
	if (ssunlikely(t->t.s == SXPREPARE))
		return 0;
	/* reselve conflicts */
	sxpreparef prepare_trigger = se_queryprepare_trigger;
	if (r->arg.recover)
		prepare_trigger = NULL;
	sxstate s = sx_prepare(&t->t, prepare_trigger, r->arg.cache);
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
se_querycommit(serequest *r)
{
	setx *t = (setx*)r->object;
	se *e = se_of(&t->o);
	/* prepare transaction for commit */
	if (ssunlikely(! sv_logcount(&t->t.log))) {
		sx_prepare(&t->t, NULL, NULL);
		sx_commit(&t->t);
		return (r->rc = 0);
	}
	int rc;
	if (sslikely(t->t.s == SXREADY || t->t.s == SXLOCK))
	{
		serequest req;
		se_requestinit(e, &req, SE_REQPREPARE, &t->o, NULL);
		req.arg.recover = r->arg.recover;
		req.arg.cache = r->arg.cache;
		rc = se_queryprepare(&req);
		if (ssunlikely(rc != 0))
			return (r->rc = rc);
	}
	assert(t->t.s == SXPREPARE);
	sx_commit(&t->t);
	/* commit */
	return (r->rc = se_querywrite(r, &t->t.log));
}

static inline int
se_queryrollback(serequest *r)
{
	setx *t = (setx*)r->object;
	sx_rollback(&t->t);
	return (r->rc = 0);
}

int se_query(serequest *r)
{
	switch (r->op) {
	case SE_REQDBSET:         return se_querydb_set(r);
	case SE_REQDBGET:         return se_querydb_get(r);
	case SE_REQTXSET:         return se_querytx_set(r);
	case SE_REQTXGET:         return se_querytx_get(r);
	case SE_REQCURSOROPEN:    return se_querycurser_open(r);
	case SE_REQCURSORGET:     return se_querycurser_get(r);
	case SE_REQCURSORDESTROY: return se_querycurser_destroy(r);
	case SE_REQBEGIN:         return se_querybegin(r);
	case SE_REQPREPARE:       return se_queryprepare(r);
	case SE_REQCOMMIT:        return se_querycommit(r);
	case SE_REQROLLBACK:      return se_queryrollback(r);
	default: assert(0);
	}
	return 0;
}
