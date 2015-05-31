
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
	uint32_t keysize = sv_size(&arg->v);
	void *key = sv_pointer(&arg->v);
	if (sslikely(arg->vlsn_generate))
		arg->vlsn = sr_seq(db->r.seq, SR_LSN);
	siquery q;
	si_queryopen(&q, &db->r, arg->cache, &db->index,
	             arg->order,
	             arg->vlsn,
	             NULL, 0, key, keysize);
	int rc = si_query(&q);
	r->rc = rc;
	r->v = q.result.v;
	si_queryclose(&q);
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
	if (sslikely(r->rc == 0))
		arg->v.v = NULL;
	return r->rc;
}

static inline int
so_querytx_get(sorequest *r)
{
	sorequestarg *arg = &r->arg;
	sodb *db = (sodb*)r->db;
	sotx *t = (sotx*)r->object;
	/* derive vlsn */
	arg->vlsn = t->t.vlsn;
	arg->vlsn_generate = 0;
	/* concurrent */
	sv result;
	r->rc = sx_get(&t->t, &db->coindex, &arg->v, &result);
	switch (r->rc) {
	case  1:
		r->v = result.v;
		break;
	case  0:
		/* storage */
		so_queryread(r);
		break;
	}
	return r->rc;
}

static inline int
so_querycursor_seek(socursor *c, sv *v)
{
	int keysize;
	char *key;
	if (v->v) {
		key = sv_pointer(v);
		keysize = sv_size(v);
	} else {
		key = NULL;
		keysize = 0;
	}
	siquery q;
	si_queryopen(&q, &c->db->r, c->cache,
	             &c->db->index, c->order, c->t.vlsn,
	             c->prefix, c->prefixsize,
	             key, keysize);
	int rc = si_query(&q);
	so_vrelease(&c->v);
	if (rc == 1) {
		assert(q.result.v != NULL);
		so_vput(&c->v, &q.result);
		so_vimmutable(&c->v);
	}
	si_queryclose(&q);
	return rc;
}

static inline int
so_querycursor_get(sorequest *r)
{
	socursor *c = (socursor*)r->object;
	if (ssunlikely(c->ready)) {
		c->ready = 0;
		r->result = &c->v;
		return (r->rc = 1);
	}
	if (ssunlikely(c->order == SS_STOP))
		return (r->rc = 0);
	if (ssunlikely(! so_vhas(&c->v)))
		return (r->rc = 0);
	r->rc = so_querycursor_seek(c, &c->v.v);
	if (ssunlikely(r->rc <= 0))
		return r->rc;
	r->result = &c->v;
	return r->rc;
}

static inline int
so_querycursor_open(sorequest *r)
{
	sorequestarg *arg = &r->arg;
	socursor *c = (socursor*)r->object;
	so *e = so_of(r->object);
	/* set cursor position */
	sx_begin(&e->xm, &c->t, arg->vlsn);
	r->rc = so_querycursor_seek(c, &c->seek);
	switch (r->rc) {
	case  1:
		r->result = &c->v;
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
	if (r->rc == 1)
		c->ready = 1;
	return r->rc;
}

static inline int
so_querycursor_destroy(sorequest *r)
{
	socursor *c = (socursor*)r->object;
	sx_rollback(&c->t);
	return 0;
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

static sxstate
so_queryprepare_trigger(sx *t, sv *v, void *arg0, void *arg1)
{
	sicache *cache = arg0;
	sodb *db = arg1;
	so *e = so_of(&db->o);
	uint64_t lsn = sr_seq(e->r.seq, SR_LSN);
	if (t->vlsn == lsn)
		return SXPREPARE;
	siquery q;
	si_queryopen(&q, &db->r, cache, &db->index,
	             SS_UPDATE, t->vlsn,
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
so_queryprepare(sorequest *r)
{
	sotx *t = (sotx*)r->object;
	if (ssunlikely(t->t.s == SXPREPARE))
		return 0;
	/* resolve conflicts */
	sxpreparef prepare_trigger = so_queryprepare_trigger;
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
so_querycommit(sorequest *r)
{
	sotx *t = (sotx*)r->object;
	so *e = so_of(&t->o);
	/* prepare transaction for commit */
	if (ssunlikely(! sv_logcount(&t->t.log))) {
		sx_prepare(&t->t, NULL, NULL);
		sx_commit(&t->t);
		return (r->rc = 0);
	}
	int rc;
	if (sslikely(t->t.s == SXREADY || t->t.s == SXLOCK))
	{
		sorequest req;
		so_requestinit(e, &req, SO_REQPREPARE, &t->o, NULL);
		req.arg.recover = r->arg.recover;
		req.arg.cache = r->arg.cache;
		rc = so_queryprepare(&req);
		if (ssunlikely(rc != 0))
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
	case SO_REQDBSET:         return so_querydb_set(r);
	case SO_REQDBGET:         return so_querydb_get(r);
	case SO_REQTXSET:         return so_querytx_set(r);
	case SO_REQTXGET:         return so_querytx_get(r);
	case SO_REQCURSOROPEN:    return so_querycursor_open(r);
	case SO_REQCURSORGET:     return so_querycursor_get(r);
	case SO_REQCURSORDESTROY: return so_querycursor_destroy(r);
	case SO_REQBEGIN:         return so_querybegin(r);
	case SO_REQPREPARE:       return so_queryprepare(r);
	case SO_REQCOMMIT:        return so_querycommit(r);
	case SO_REQROLLBACK:      return so_queryrollback(r);
	default: assert(0);
	}
	return 0;
}
