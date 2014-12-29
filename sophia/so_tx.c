
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

int so_txdbset(sodb *db, uint8_t flags, va_list args)
{
	/* validate call */
	sov *o = va_arg(args, sov*);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return -1;
	}
	sv *ov = &o->v;
	if (srunlikely(ov->v == NULL)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	soobj *parent = o->parent;
	if (srunlikely(parent != &db->o)) {
		sr_error(&db->e->error, "%s", "bad object parent");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	int status = so_status(&db->status);
	if (srunlikely(! so_statusactive_is(status)))
		goto error;
	if (srunlikely(status == SO_RECOVER))
		goto error;

	/* prepare object */
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
	sxstate s = sx_setstmt(&db->e->xm, &db->coindex, &vp);
	int rc = 1; /* rlb */
	switch (s) {
	case SXWAIT: rc = 2;
	case SXROLLBACK:
		so_objdestroy(&o->o);
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

	/* log write */
	svlogv lv;
	lv.id = db->ctl.id;
	lv.next = 0;
	svinit(&lv.v, &sv_vif, v, NULL);
	svlog log;
	sv_loginit(&log);
	sv_logadd(&log, db->r.a, &lv, db);
	svlogindex *logindex = (svlogindex*)log.index.s;
	sltx tl;
	sl_begin(&db->e->lp, &tl);
	sl_prepare(&db->e->lp, &log);
	rc = sl_write(&tl, &log);
	if (srunlikely(rc == -1)) {
		sl_rollback(&tl);
		goto error;
	}
	v->log = tl.l;
	sl_commit(&tl);

	/* commit */
	uint64_t vlsn = sx_vlsn(&db->e->xm);
	uint64_t now = sr_utime();
	sitx tx;
	si_begin(&tx, &db->r, &db->index, vlsn, now,
	         &log, logindex);
	si_write(&tx, 0);
	si_commit(&tx);

	so_objdestroy(&o->o);
	return 0;
error:
	so_objdestroy(&o->o);
	return -1;
}

void *so_txdbget(sodb *db, uint64_t vlsn, va_list args)
{
	/* validate call */
	sov *o = va_arg(args, sov*);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		return NULL;
	}
	uint32_t keysize = svkeysize(&o->v);
	void *key = svkey(&o->v);
	if (srunlikely(key == NULL)) {
		sr_error(&db->e->error, "%s", "bad arguments");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	soobj *parent = o->parent;
	if (srunlikely(parent != &db->o)) {
		sr_error(&db->e->error, "%s", "bad object parent");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	if (srunlikely(! so_dbactive(db)))
		goto error;

	sx_getstmt(&db->e->xm, &db->coindex);
	if (srlikely(vlsn == 0))
		vlsn = sr_seq(db->r.seq, SR_LSN);

	sicache cache;
	si_cacheinit(&cache, &db->e->a_cursorcache);
	siquery q;
	si_queryopen(&q, &db->r, &cache, &db->index,
	             SR_EQ, vlsn, key, keysize);
	sv result;
	int rc = si_query(&q);
	if (rc == 1) {
		rc = si_querydup(&q, &result);
	}
	si_queryclose(&q);
	si_cachefree(&cache, &db->r);

	so_objdestroy(&o->o);
	if (srunlikely(rc <= 0))
		return NULL;
	soobj *ret = so_vdup(db->e, &db->o, &result);
	if (srunlikely(ret == NULL))
		sv_vfree(&db->e->a, (svv*)result.v);
	return ret;
error:
	so_objdestroy(&o->o);
	return NULL;
}

static int
so_txdo(soobj *obj, uint8_t flags, va_list args)
{
	sotx *t = (sotx*)obj;
	so *e = t->e;

	/* validate call */
	sov *o = va_arg(args, sov*);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		sr_error_recoverable(&e->error);
		return -1;
	}
	sv *ov = &o->v;
	if (srunlikely(ov->v == NULL)) {
		sr_error(&e->error, "%s", "bad arguments");
		sr_error_recoverable(&e->error);
		goto error;
	}
	soobj *parent = o->parent;
	if (parent == NULL || parent->id != SODB) {
		sr_error(&e->error, "%s", "bad object parent");
		sr_error_recoverable(&e->error);
		goto error;
	}
	sodb *db = (sodb*)parent;
	if (t->t.s == SXPREPARE) {
		sr_error(&e->error, "%s", "transaction is in 'prepare' state (read-only)");
		sr_error_recoverable(&e->error);
		goto error;
	}
	if (srunlikely(! so_dbactive(db)))
		goto error;

	/* prepare object */
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

	svv *v = sv_valloc(db->r.a, &vp);
	if (srunlikely(v == NULL)) {
		sr_error(&db->e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&db->e->error);
		goto error;
	}
	v->log = o->log;
	int rc = sx_set(&t->t, &db->coindex, v);
	so_objdestroy(&o->o);
	return rc;
error:
	so_objdestroy(&o->o);
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
	sotx *t = (sotx*)obj;
	so *e = t->e;

	/* validate call */
	sov *o = va_arg(args, sov*);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	void *key = svkey(&o->v);
	if (srunlikely(key == NULL)) {
		sr_error(&e->error, "%s", "bad arguments");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	soobj *parent = o->parent;
	if (parent == NULL || parent->id != SODB) {
		sr_error(&e->error, "%s", "bad object parent");
		sr_error_recoverable(&e->error);
		goto error;
	}
	sodb *db = (sodb*)parent;
	if (srunlikely(! so_dbactive(db)))
		goto error;

	soobj *ret;
	sv result;
	int rc = sx_get(&t->t, &db->coindex, &o->v, &result);
	switch (rc) {
	case -1:
	case  2: /* delete */
		so_objdestroy(&o->o);
		return NULL;
	case  1:
		ret = so_vdup(e, &db->o, &result);
		if (srunlikely(ret == NULL))
			sv_vfree(&db->e->a, (svv*)result.v);
		so_objdestroy(&o->o);
		return ret;
	}

	sicache cache;
	si_cacheinit(&cache, &db->e->a_cursorcache);
	siquery q;
	si_queryopen(&q, &db->r, &cache, &db->index,
	             SR_EQ, t->t.vlsn,
	             key, svkeysize(&o->v));
	rc = si_query(&q);
	if (rc == 1) {
		rc = si_querydup(&q, &result);
	}
	si_queryclose(&q);
	si_cachefree(&cache, &db->r);

	so_objdestroy(&o->o);
	if (srunlikely(rc <= 0))
		return NULL;
	ret = so_vdup(e, &db->o, &result);
	if (srunlikely(ret == NULL))
		sv_vfree(&e->a, (svv*)result.v);
	return ret;
error:
	so_objdestroy(&o->o);
	return NULL;
}

static inline void
so_txend(sotx *t)
{
	so_objindex_destroy(&t->logcursor);
	so_objindex_unregister(&t->e->tx, &t->o);
	sr_free(&t->e->a_tx, t);
}

static int
so_txrollback(soobj *o)
{
	sotx *t = (sotx*)o;
	sx_rollback(&t->t);
	sx_end(&t->t);
	so_txend(t);
	return 0;
}

static sxstate
so_txprepare_trigger(sx *t, sv *v, void *arg0, void *arg1)
{
	sotx *te = arg0;
	sodb *db = arg1;
	uint64_t lsn = sr_seq(te->e->r.seq, SR_LSN);
	if (t->vlsn == lsn)
		return SXPREPARE;
	sicache cache;
	si_cacheinit(&cache, &db->e->a_cursorcache);
	siquery q;
	si_queryopen(&q, &db->r, &cache, &db->index,
	             SR_UPDATE, t->vlsn,
	             svkey(v), svkeysize(v));
	int rc;
	rc = si_query(&q);
	si_queryclose(&q);
	si_cachefree(&cache, &db->r);
	if (srunlikely(rc))
		return SXROLLBACK;
	return SXPREPARE;
}

static int
so_txprepare(soobj *o, va_list args srunused)
{
	sotx *t = (sotx*)o;
	so *e = t->e;
	int status = so_status(&e->status);
	if (srunlikely(! so_statusactive_is(status)))
		return -1;
	if (t->t.s == SXPREPARE)
		return 0;
	/* resolve conflicts */
	sxpreparef prepare_trigger = so_txprepare_trigger;
	if (srunlikely(status == SO_RECOVER))
		prepare_trigger = NULL;
	sxstate s = sx_prepare(&t->t, prepare_trigger, t);
	if (s == SXWAIT)
		return 2;
	if (s == SXROLLBACK) {
		so_txrollback(&t->o);
		return 1;
	}
	assert(s == SXPREPARE);
	if (e->ctl.commit_lsn || status == SO_RECOVER)
		return 0;
	/* assign lsn */
	sl_prepare(&e->lp, &t->t.log);
	return 0;
}

static int
so_txcommit(soobj *o, va_list args)
{
	sotx *t = (sotx*)o;
	so *e = t->e;
	int status = so_status(&e->status);
	if (srunlikely(! so_statusactive_is(status)))
		return -1;

	/* prepare transaction for commit */
	assert (t->t.s == SXPREPARE || t->t.s == SXREADY);
	int rc;
	if (t->t.s == SXREADY) {
		rc = so_txprepare(o, args);
		if (srunlikely(rc != 0))
			return rc;
	}
	assert(t->t.s == SXPREPARE);

	if (srunlikely(! sv_logcount(&t->t.log))) {
		sx_commit(&t->t);
		sx_end(&t->t);
		so_txend(t);
		return 0;
	}
	sx_commit(&t->t);

	/* synchronize lsn */
	sl_follow(&e->lp, &t->t.log);

	/* log commit */
	if (status == SO_ONLINE) {
		sltx tl;
		sl_begin(&e->lp, &tl);
		rc = sl_write(&tl, &t->t.log);
		if (srunlikely(rc == -1)) {
			sl_rollback(&tl);
			so_txrollback(&t->o);
			return -1;
		}
		sl_commit(&tl);
	}

	/* prepare commit */
	int check_if_exists;
	uint64_t vlsn;
	if (srunlikely(status == SO_RECOVER)) {
		check_if_exists = 1;
		vlsn = sr_seq(e->r.seq, SR_LSN);
	} else {
		check_if_exists = 0;
		vlsn = sx_vlsn(&e->xm);
	}

	/* multi-index commit */
	uint64_t now = sr_utime();

	svlogindex *i   = (svlogindex*)t->t.log.index.s;
	svlogindex *end = (svlogindex*)t->t.log.index.p;
	while (i < end) {
		sodb *db = i->ptr;
		sitx ti;
		si_begin(&ti, &db->r, &db->index, vlsn, now, &t->t.log, i);
		si_write(&ti, check_if_exists);
		si_commit(&ti);
		i++;
	}
	sx_end(&t->t);

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
	.object   = NULL,
	.type     = so_txtype
};

soobj *so_txnew(so *e)
{
	sotx *t = sr_malloc(&e->a_tx, sizeof(sotx));
	if (srunlikely(t == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	so_objinit(&t->o, SOTX, &sotxif, &e->o);
	so_objindex_init(&t->logcursor);
	t->e = e;
	sx_begin(&e->xm, &t->t, 0);
	so_objindex_register(&e->tx, &t->o);
	return &t->o;
}
