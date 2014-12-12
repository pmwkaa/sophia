
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
#include <libse.h>
#include <libso.h>

int so_txdbset(sodb *db, uint8_t flags, va_list args)
{
	int rc;
	int status = so_status(&db->status);
	if (srunlikely(! so_statusactive_is(status)))
		return -1;
	if (srunlikely(status == SO_RECOVER))
		return -1;
	soobj *o = va_arg(args, soobj*);
	sv *ov = NULL;
	if (srunlikely(o->id != SOV)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	ov = &((sov*)o)->v;
	if (srunlikely(ov->v == NULL)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	svlocal l;
	l.flags       = flags;
	l.lsn         = 0;
	l.key         = svkey(ov);
	l.keysize     = svkeysize(ov);
	l.value       = svvalue(ov);
	l.valuesize   = svvaluesize(ov);
	l.valueoffset = 0;
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);

	/* ensure quota */
	sr_quota(&db->e->quota, SR_QADD, sv_vsizeof(&vp));

	/* concurrency */
	smstate s = sm_set_stmt(&db->mvcc, &vp);
	rc = 1; /* rlb */
	switch (s) {
	case SMWAIT: rc = 2;
	case SMROLLBACK:
		so_objdestroy(o);
		return rc;
	default:
		break;
	}
	svv *v = sv_valloc(db->r.a, &vp);
	if (srunlikely(v == NULL)) {
		sr_error(&db->e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	svinit(&vp, &sv_vif, v, NULL);
	/* log write */
	svlog log;
	sv_loginit(&log);
	sv_logadd(&log, db->r.a, &vp);
	sltx tl;
	sl_begin(&db->e->lp, &tl, db->ctl.id);
	sl_prepare(&db->e->lp, &log);
	rc = sl_write(&tl, &log);
	if (srunlikely(rc == -1)) {
		sl_rollback(&tl);
		goto error;
	}
	v->log = tl.l;
	sl_commit(&tl);
	uint64_t vlsn = so_dbvlsn(db);
	sitx tx;
	si_begin(&tx, &db->r, &db->index, vlsn, &log);
	si_write(&tx, 0);
	si_commit(&tx);
	so_objdestroy(o);
	return 0;
error:
	so_objdestroy(o);
	return -1;
}

void *so_txdbget(sodb *db, va_list args)
{
	if (srunlikely(! so_dbactive(db)))
		return NULL;
	soobj *o = va_arg(args, soobj*);
	if (srunlikely(o->id != SOV)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	sov *v = (sov*)o;
	void *key = svkey(&v->v);
	uint32_t keysize = svkeysize(&v->v);
	if (srunlikely(key == NULL)) {
		so_objdestroy(o);
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	sm_get_stmt(&db->mvcc);
	uint64_t vlsn = sr_seq(db->r.seq, SR_LSN) - 1;
	sv result;
	siquery q;
	si_queryopen(&q, &db->r, &db->index, SR_EQ, vlsn, key, keysize);
	int rc = si_query(&q);
	if (rc) {
		rc = si_querydup(&q, &result);
	}
	si_queryclose(&q);
	so_objdestroy(o);
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
	sodb *db = t->db;
	svv *v;
	int rc;
	/* prepare object */
	soobj *o = va_arg(args, soobj*);
	sv *ov = NULL;
	if (srunlikely(o->id != SOV)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	ov = &((sov*)o)->v;
	if (srunlikely(ov->v == NULL)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	if (t->t.s == SMPREPARE) {
		sr_error(&db->e->error, "%s", "transaction is in 'prepare' state (read-only)");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	svlocal l;
	l.flags       = flags;
	l.lsn         = svlsn(ov);
	l.key         = svkey(ov);
	l.keysize     = svkeysize(ov);
	l.value       = svvalue(ov);
	l.valuesize   = svvaluesize(ov);
	l.valueoffset = 0;
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);

	/* ensure quota */
	sr_quota(&db->e->quota, SR_QADD, sv_vsizeof(&vp));

	v = sv_valloc(db->r.a, &vp);
	if (srunlikely(v == NULL)) {
		sr_error(&db->e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	v->log = ((sov*)o)->log;
	rc = sm_set(&t->t, v);
	so_objdestroy(o);
	return rc;
error:
	so_objdestroy(o);
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
	if (srunlikely(o->id != SOV)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	sov *v = (sov*)o;
	void *key = svkey(&v->v);
	if (srunlikely(key == NULL)) {
		so_objdestroy(o);
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	soobj *ret;
	sv result;
	int rc;
	rc = sm_get(&t->t, &v->v, &result);
	switch (rc) {
	case -1:
	case  2: /* delete */
		so_objdestroy(o);
		return NULL;
	case  1:
		so_objdestroy(o);
		ret = so_vdup(db->e, &result);
		if (srunlikely(ret == NULL))
			sv_vfree(&db->e->a, (svv*)result.v);
		return ret;
		}
	siquery q;
	si_queryopen(&q, &t->db->r, &t->db->index, SR_EQ,
	             t->t.vlsn,
	             key, svkeysize(&v->v));
	rc = si_query(&q);
	if (rc) {
		rc = si_querydup(&q, &result);
	}
	si_queryclose(&q);
	so_objdestroy(o);
	if (srunlikely(rc <= 0))
		return NULL;
	ret = so_vdup(db->e, &result);
	if (srunlikely(ret == NULL))
		sv_vfree(&db->e->a, (svv*)result.v);
	return ret;
}

static inline void
so_txend(sotx *t)
{
	so_objindex_destroy(&t->logcursor);
	so_objindex_unregister(&t->db->tx, &t->o);
	sr_free(&t->db->e->a_tx, t);
}

static int
so_txrollback(soobj *o)
{
	sotx *t = (sotx*)o;
	sm_rollback(&t->t);
	sm_end(&t->t);
	so_txend(t);
	return 0;
}

static smstate
so_txprepare_trigger(smtx *t, sv *v, void *arg)
{
	sotx *te = arg;
	uint64_t lsn = sr_seq(te->db->r.seq, SR_LSN);
	if ((lsn - 1) == t->vlsn)
		return SMPREPARE;
	siquery q;
	si_queryopen(&q, &te->db->r, &te->db->index, SR_UPDATE,
	             t->vlsn,
	             svkey(v), svkeysize(v));
	int rc;
	rc = si_query(&q);
	si_queryclose(&q);
	if (srunlikely(rc))
		return SMROLLBACK;
	return SMPREPARE;
}

static int
so_txprepare(soobj *o, va_list args srunused)
{
	sotx *t  = (sotx*)o;
	sodb *db = t->db;
	int status = so_status(&db->status);
	if (srunlikely(! so_statusactive_is(status)))
		return -1;
	if (t->t.s == SMPREPARE)
		return 0;
	/* resolve conflicts */
	smpreparef prepare_trigger = so_txprepare_trigger;
	if (srunlikely(status == SO_RECOVER))
		prepare_trigger = NULL;
	smstate s = sm_prepare(&t->t, prepare_trigger, t);
	if (s == SMWAIT)
		return 2;
	if (s == SMROLLBACK) {
		so_txrollback(&t->o);
		return 1;
	}
	assert(s == SMPREPARE);
	if (db->e->ctl.commit_lsn || status == SO_RECOVER)
		return 0;
	/* assign lsn */
	sl_prepare(&db->e->lp, &t->t.log);
	return 0;
}

static int
so_txcommit(soobj *o, va_list args)
{
	sotx *t  = (sotx*)o;
	sodb *db = t->db;
	int status = so_status(&db->status);
	if (srunlikely(! so_statusactive_is(status)))
		return -1;

	/* prepare transaction for commit */
	assert (t->t.s == SMPREPARE || t->t.s == SMREADY);
	int rc;
	if (t->t.s == SMREADY) {
		rc = so_txprepare(o, args);
		if (srunlikely(rc != 0))
			return rc;
	}
	assert(t->t.s == SMPREPARE);

	if (srunlikely(! sv_logn(&t->t.log))) {
		sm_commit(&t->t);
		sm_end(&t->t);
		so_txend(t);
		return 0;
	}
	sm_commit(&t->t);

	/* synchronize lsn */
	sl_follow(&db->e->lp, &t->t.log);

	/* log commit */
	if (status == SO_ONLINE) {
		sltx tl;
		sl_begin(&db->e->lp, &tl, db->ctl.id);
		rc = sl_write(&tl, &t->t.log);
		if (srunlikely(rc == -1)) {
			sl_rollback(&tl);
			so_txrollback(&t->o);
			return -1;
		}
		sl_commit(&tl);
	}

	/* index commit */
	int check_if_exists;
	uint64_t vlsn;
	if (srunlikely(status == SO_RECOVER)) {
		check_if_exists = 1;
		vlsn = sr_seq(db->r.seq, SR_LSN) - 1;
	} else {
		check_if_exists = 0;
		vlsn = so_dbvlsn(db);
	}
	sitx ti;
	si_begin(&ti, &db->r, &db->index, vlsn, &t->t.log);
	si_write(&ti, check_if_exists);
	si_commit(&ti);
	sm_end(&t->t);

	so_txend(t);
	return 0;
}

static void*
so_txctl(soobj *obj, va_list args)
{
	sotx *t = (sotx*)obj;
	char *name = va_arg(args, char*);
	if (strcmp(name, "log_cursor") == 0)
		return so_logcursor_new(t);
	return NULL;
}

static void*
so_txobject(soobj *o, va_list args srunused)
{
	sotx *t = (sotx*)o;
	return so_objobject(&t->db->o);
}

static void*
so_txtype(soobj *o srunused, va_list args srunused) {
	return "transaction";
}

static soobjif sotxif =
{
	.ctl      = so_txctl,
	.open     = NULL,
	.destroy  = so_txrollback,
	.error    = NULL,
	.set      = so_txset,
	.del      = so_txdelete,
	.get      = so_txget,
	.begin    = NULL,
	.prepare  = so_txprepare,
	.commit   = so_txcommit,
	.rollback = so_txrollback,
	.cursor   = NULL,
	.object   = so_txobject,
	.type     = so_txtype
};

soobj *so_txnew(sodb *db)
{
	so *e = db->e;
	sotx *t = sr_malloc(&e->a_tx, sizeof(sotx));
	if (srunlikely(t == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	so_objinit(&t->o, SOTX, &sotxif, &e->o);
	so_objindex_init(&t->logcursor);
	t->db = db;
	sm_begin(&db->mvcc, &t->t);
	so_objindex_register(&db->tx, &t->o);
	return &t->o;
}
