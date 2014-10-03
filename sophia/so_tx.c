
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libso.h>
#include <sophia.h>

int so_txdbset(sodb *db, uint8_t flags, va_list args)
{
	int rc;
	if (srunlikely(! so_dbactive(db)))
		return -1;
	soobj *o = va_arg(args, soobj*);
	sv *ov = NULL;
	if (srunlikely(o->oid != SOV))
		return -1;
	ov = &((sov*)o)->v;
	if (srunlikely(ov->v == NULL))
		goto error;
	svlocal l;
	l.lsn         = 0;
	l.flags       = flags;
	l.key         = svkey(ov);
	l.keysize     = svkeysize(ov);
	l.value       = svvalue(ov);
	l.valuesize   = svvaluesize(ov);
	l.valueoffset = 0;
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);
	svv *v = sv_valloc(db->r.a, &vp);
	if (srunlikely(v == NULL))
		goto error;
	svinit(&vp, &sv_vif, v, NULL);
	/* update */
	sm_lock(&db->mvcc);
	smstate s = sm_set_stmt(&db->mvcc, v);
	rc = 1; /* rlb */
	switch (s) {
	case SMWAIT: rc = 2;
	case SMROLLBACK:
		sm_unlock(&db->mvcc);
		sv_vfree(db->r.a, v);
		sp_destroy(o);
		return rc;
	default:
		break;
	}
	/* log write */
	sltx tl;
	sl_begin(&db->lp, &tl);
	svlog log;
	sv_loginit(&log);
	sv_logadd(&log, NULL, &vp);
	rc = sl_write(&tl, &log);
	if (srunlikely(rc == -1)) {
		sl_rollback(&tl);
		sm_unlock(&db->mvcc);
		goto error;
	}
	v->log = tl.l;
	sl_commit(&tl);
	uint64_t lsvn = sm_lsvn(&db->mvcc);
	sitx tx;
	si_begin(&tx, &db->r, &db->index, lsvn, NULL, v);
	rc = si_write(&tx);
	si_commit(&tx);
	sm_unlock(&db->mvcc);
	sp_destroy(o);
	return rc;
error:
	sp_destroy(o);
	return -1;
}

void *so_txdbget(sodb *db, va_list args)
{
	if (srunlikely(! so_dbactive(db)))
		return NULL;
	soobj *o = va_arg(args, soobj*);
	if (srunlikely(o->oid != SOV))
		return NULL;
	sov *v = (sov*)o;
	void *key = svkey(&v->v);
	uint32_t keysize = svkeysize(&v->v);
	if (srunlikely(key == NULL)) {
		sp_destroy(o);
		return NULL;
	}
	sm_get_stmt(&db->mvcc);
	uint64_t lsvn = sr_seq(db->r.seq, SR_LSN) - 1;
	sv result;
	siquery q;
	si_queryopen(&q, &db->r, &db->index, SR_EQ, lsvn, key, keysize);
	int rc = si_query(&q);
	if (rc) {
		rc = si_querydup(&q, &result);
	}
	si_queryclose(&q);
	sp_destroy(o);
	if (srunlikely(rc <= 0))
		return NULL;
	soobj *ret = so_vdup(db->e, &result);
	if (srunlikely(ret == NULL))
		sv_vfree(&db->e->a, (svv*)result.v);
	return ret;
}

static int
so_txdo(soobj *obj, uint8_t flags, va_list args)
{
	sotx *t = (sotx*)obj;
	svv *v;
	int rc;
	if (srunlikely(t->db->mode == SO_RECOVER)) {
		sv *recover_v = va_arg(args, sv*);
		v = sv_valloc(t->db->r.a, recover_v);
		if (srunlikely(v == NULL))
			return -1;
		rc = sm_set(&t->t, v);
		return rc;
	}
	/* prepare object */
	soobj *o = va_arg(args, soobj*);
	sv *ov = NULL;
	if (srunlikely(o->oid != SOV))
		return -1;
	ov = &((sov*)o)->v;
	if (srunlikely(ov->v == NULL))
		goto error;
	svlocal l;
	l.lsn         = 0;
	l.flags       = flags;
	l.key         = svkey(ov);
	l.keysize     = svkeysize(ov);
	l.value       = svvalue(ov);
	l.valuesize   = svvaluesize(ov);
	l.valueoffset = 0;
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);
	v = sv_valloc(t->db->r.a, &vp);
	if (srunlikely(v == NULL))
		goto error;
	sm_lock(&t->db->mvcc);
	rc = sm_set(&t->t, v);
	sm_unlock(&t->db->mvcc);
	sp_destroy(o);
	return rc;
error:
	sp_destroy(o);
	return -1;
}

static int
so_txset(soobj *o, va_list args)
{
	return so_txdo(o, SVSET, args);
}

static int
so_txdelete(soobj *o, va_list args)
{
	return so_txdo(o, SVDELETE, args);
}

static void*
so_txget(soobj *obj, va_list args)
{
	sotx *t  = (sotx*)obj;
	sodb *db = t->db;
	if (srunlikely(! so_dbactive(db)))
		return NULL;
	soobj *o = va_arg(args, soobj*);
	if (srunlikely(o->oid != SOV))
		return NULL;
	sov *v = (sov*)o;
	void *key = svkey(&v->v);
	if (srunlikely(key == NULL)) {
		sp_destroy(o);
		return NULL;
	}
	sm_lock(&t->db->mvcc);
	soobj *ret;
	sv result;
	int rc;
	rc = sm_get(&t->t, &v->v, &result);
	sm_unlock(&t->db->mvcc);
	switch (rc) {
	case -1:
	case  2: /* delete */
		sp_destroy(o);
		return NULL;
	case  1:
		sp_destroy(o);
		ret = so_vdup(db->e, &result);
		if (srunlikely(ret == NULL))
			sv_vfree(&db->e->a, (svv*)result.v);
		return ret;
		}
	siquery q;
	si_queryopen(&q, &t->db->r, &t->db->index, SR_EQ,
	             t->t.lsvn,
	             key, svkeysize(&v->v));
	rc = si_query(&q);
	if (rc) {
		rc = si_querydup(&q, &result);
	}
	si_queryclose(&q);
	sp_destroy(o);
	if (srunlikely(rc <= 0))
		return NULL;
	ret = so_vdup(db->e, &result);
	if (srunlikely(ret == NULL))
		sv_vfree(&db->e->a, (svv*)result.v);
	return ret;
}

static smstate
so_txprepare(smtx *t, sv *v, void *arg)
{
	sotx *te = arg;
	uint64_t lsn = sr_seq(te->db->r.seq, SR_LSN);
	if ((lsn - 1) == t->lsvn) /* last txn' lsn? */
		return SMPREPARE;
	siquery q;
	si_queryopen(&q, &te->db->r, &te->db->index, SR_UPDATE,
	             t->lsvn,
	             svkey(v), svkeysize(v));
	int rc;
	rc = si_query(&q);
	si_queryclose(&q);
	if (srunlikely(rc))
		return SMROLLBACK;
	return SMPREPARE;
}

static int
so_txrollback(soobj *o)
{
	sotx *t = (sotx*)o;
	sm_lock(&t->db->mvcc);
	sm_rollback(&t->t);
	sm_unlock(&t->db->mvcc);
	sm_end(&t->t);
	so_objindex_unregister(&t->db->tx, &t->o);
	sr_free(&t->db->e->a, t);
	return 0;
}

static int
so_txcommit_recover(soobj *o, va_list args)
{
	sotx *t  = (sotx*)o;
	sodb *db = t->db;
	so *e    = t->db->e;

	uint64_t lsn = va_arg(args, uint64_t);
	sl *log = va_arg(args, sl*);
	(void)log;

	sm_lock(&db->mvcc);
	smstate s = sm_prepare(&t->t, so_txprepare, t);
	sm_unlock(&db->mvcc);
	if (s == SMWAIT)
		return 2;
	if (s == SMROLLBACK) {
		so_txrollback(&t->o);
		return 1;
	}
	assert(s == SMPREPARE);

	if (srunlikely(! sv_logn(&t->t.log))) {
		sm_lock(&db->mvcc);
		sm_commit(&t->t);
		sm_end(&t->t);
		sm_unlock(&db->mvcc);
		so_objindex_unregister(&db->tx, &t->o);
		sr_free(&e->a, t);
		return 0;
	}

	/* recover lsn number */
	sriter i;
	sr_iterinit(&i, &sr_bufiter, &db->r);
	sr_iteropen(&i, &t->t.log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		((svv*)v->v)->log = log;
		svlsnset(v, lsn);
	}

	/* database write */
	sm_lock(&db->mvcc);
	sm_commit(&t->t);
	uint64_t lsvn = sm_lsvn(&db->mvcc);
	int rc;
	sitx ti;
	si_begin(&ti, &db->r, &db->index, lsvn, &t->t.log, NULL);
	rc = si_writelog(&ti);
	assert(rc == 0);
	si_commit(&ti);
	sm_unlock(&db->mvcc);
	sm_end(&t->t);

	so_objindex_unregister(&db->tx, &t->o);
	sr_free(&e->a, t);
	return 0;
}

static int
so_txcommit(soobj *o, va_list args)
{
	sotx *t  = (sotx*)o;
	sodb *db = t->db;
	so *e    = t->db->e;
	if (e->mode == SO_RECOVER)
		return so_txcommit_recover(o, args);
	sm_lock(&db->mvcc);
	smstate s = sm_prepare(&t->t, so_txprepare, t);
	sm_unlock(&db->mvcc);
	if (s == SMWAIT) {
		return 2;
	}
	if (s == SMROLLBACK) {
		so_txrollback(&t->o);
		return 1;
	}
	assert(s == SMPREPARE);
	if (srunlikely(! sv_logn(&t->t.log))) {
		sm_lock(&db->mvcc);
		sm_commit(&t->t);
		sm_end(&t->t);
		sm_unlock(&db->mvcc);
		so_objindex_unregister(&db->tx, &t->o);
		sr_free(&e->a, t);
		return 0;
	}

	/* commit data */
	sltx tl;
	sl_begin(&db->lp, &tl);
	int rc = sl_write(&tl, &t->t.log);
	if (srunlikely(rc == -1)) {
		sl_rollback(&tl);
		so_txrollback(&t->o);
		return -1;
	}

	sriter i;
	sr_iterinit(&i, &sr_bufiter, &db->r);
	sr_iteropen(&i, &t->t.log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		((svv*)v->v)->log = tl.l;
	}

	sm_lock(&db->mvcc);
	sm_commit(&t->t);
	sl_commit(&tl); 
	uint64_t lsvn = sm_lsvn(&db->mvcc);
	sitx ti;
	si_begin(&ti, &db->r, &db->index, lsvn, &t->t.log, NULL);
	rc = si_writelog(&ti);
	assert(rc == 0);
	si_commit(&ti);
	sm_unlock(&db->mvcc);
	sm_end(&t->t);

	so_objindex_unregister(&db->tx, &t->o);
	sr_free(&e->a, t);
	return 0;
}

static void*
so_txtype(soobj *o srunused, va_list args srunused) {
	return "transaction";
}

static soobjif sotxif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = so_txrollback,
	.set      = so_txset,
	.del      = so_txdelete,
	.get      = so_txget,
	.begin    = NULL,
	.commit   = so_txcommit,
	.rollback = so_txrollback,
	.cursor   = NULL,
	.object   = NULL,
	.type     = so_txtype,
	.copy     = NULL
};

soobj *so_txnew(sodb *db)
{
	so *e = db->e;
	sotx *t = sr_malloc(&e->a, sizeof(sotx));
	if (srunlikely(t == NULL))
		return NULL;
	so_objinit(&t->o, SOTX, &sotxif);
	t->db = db;
	int rc = sm_begin(&db->mvcc, &t->t);
	if (srunlikely(rc == -1)) {
		sr_free(&e->a, t);
		return NULL;
	}
	so_objindex_register(&db->tx, &t->o);
	return &t->o;
}
