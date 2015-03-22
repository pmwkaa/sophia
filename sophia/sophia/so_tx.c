
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
	so *e = so_of(&db->o);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return -1;
	}
	sv *ov = &o->v;
	if (srunlikely(ov->v == NULL)) {
		sr_error(&e->error, "%s", "bad arguments");
		goto error;
	}
	soobj *parent = o->parent;
	if (srunlikely(parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		goto error;
	}
	if (srunlikely(! so_online(&db->status)))
		goto error;

	/* prepare object */
	svlocal l;
	l.flags       = flags;
	l.lsn         = 0;
	l.key         = svkey(ov);
	l.keysize     = svkeysize(ov);
	l.value       = svvalue(ov);
	l.valuesize   = svvaluesize(ov);
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);

	/* ensure quota */
	sr_quota(&e->quota, SR_QADD, sv_vsizeof(&vp));

	/* concurrency */
	sxstate s = sx_setstmt(&e->xm, &db->coindex, &vp);
	int rc = 1; /* rlb */
	switch (s) {
	case SXLOCK: rc = 2;
	case SXROLLBACK:
		so_objdestroy(&o->o);
		return rc;
	default:
		break;
	}
	svv *v = sv_valloc(db->r.a, &vp);
	if (srunlikely(v == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
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
	sl_begin(&e->lp, &tl);
	sl_prepare(&e->lp, &log, 0);
	rc = sl_write(&tl, &log);
	if (srunlikely(rc == -1)) {
		sl_rollback(&tl);
		goto error;
	}
	v->log = tl.l;
	sl_commit(&tl);

	/* commit */
	uint64_t vlsn = sx_vlsn(&e->xm);
	uint64_t now = sr_utime();
	sitx tx;
	si_begin(&tx, &db->r, &db->index, vlsn, now, &log, logindex);
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
	so *e = so_of(&db->o);
	/* validate call */
	sov *o = va_arg(args, sov*);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	uint32_t keysize = svkeysize(&o->v);
	void *key = svkey(&o->v);
	if (srunlikely(key == NULL)) {
		sr_error(&e->error, "%s", "bad arguments");
		goto error;
	}
	soobj *parent = o->parent;
	if (srunlikely(parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		goto error;
	}
	if (srunlikely(! so_online(&db->status)))
		goto error;

	sx_getstmt(&e->xm, &db->coindex);
	if (srlikely(vlsn == 0))
		vlsn = sr_seq(db->r.seq, SR_LSN);

	sicache cache;
	si_cacheinit(&cache, &e->a_cursorcache);
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
	soobj *ret = so_vdup(e, &db->o, &result);
	if (srunlikely(ret == NULL))
		sv_vfree(&e->a, (svv*)result.v);
	return ret;
error:
	so_objdestroy(&o->o);
	return NULL;
}

static int
so_txdo(soobj *obj, uint8_t flags, va_list args)
{
	sotx *t = (sotx*)obj;
	so *e = so_of(obj);

	/* validate call */
	sov *o = va_arg(args, sov*);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return -1;
	}
	sv *ov = &o->v;
	if (srunlikely(ov->v == NULL)) {
		sr_error(&e->error, "%s", "bad arguments");
		goto error;
	}
	soobj *parent = o->parent;
	if (parent == NULL || parent->id != SODB) {
		sr_error(&e->error, "%s", "bad object parent");
		goto error;
	}
	if (t->t.s == SXPREPARE) {
		sr_error(&e->error, "%s", "transaction is in 'prepare' state (read-only)");
		goto error;
	}

	/* validate database status */
	sodb *db = (sodb*)parent;
	int status = so_status(&db->status);
	switch (status) {
	case SO_ONLINE:
	case SO_RECOVER:
		break;
	case SO_SHUTDOWN:
		if (srunlikely(! so_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	default: goto error;
	}

	/* prepare object */
	svlocal l;
	l.flags       = flags;
	l.lsn         = svlsn(ov);
	l.key         = svkey(ov);
	l.keysize     = svkeysize(ov);
	l.value       = svvalue(ov);
	l.valuesize   = svvaluesize(ov);
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);

	/* ensure quota */
	sr_quota(&e->quota, SR_QADD, sv_vsizeof(&vp));

	svv *v = sv_valloc(db->r.a, &vp);
	if (srunlikely(v == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
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
	so *e = so_of(obj);

	/* validate call */
	sov *o = va_arg(args, sov*);
	if (srunlikely(o->o.id != SOV)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	void *key = svkey(&o->v);
	if (srunlikely(key == NULL)) {
		sr_error(&e->error, "%s", "bad arguments");
		return NULL;
	}
	soobj *parent = o->parent;
	if (parent == NULL || parent->id != SODB) {
		sr_error(&e->error, "%s", "bad object parent");
		goto error;
	}

	/* validate database status */
	sodb *db = (sodb*)parent;
	int status = so_status(&db->status);
	switch (status) {
	case SO_ONLINE:
	case SO_RECOVER:
		break;
	case SO_SHUTDOWN:
		if (srunlikely(! so_dbvisible(db, t->t.id))) {
			sr_error(&e->error, "%s", "database is invisible for the transaction");
			goto error;
		}
		break;
	default: goto error;
	}

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
			sv_vfree(&e->a, (svv*)result.v);
		so_objdestroy(&o->o);
		return ret;
	}

	sicache cache;
	si_cacheinit(&cache, &e->a_cursorcache);
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
	so *e = so_of(&t->o);
	so_dbunbind(e, t->t.id);
	so_objindex_unregister(&e->tx, &t->o);
	sr_free(&e->a_tx, t);
}

static int
so_txrollback(soobj *o, va_list args srunused)
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
	sotx *te srunused = arg0;
	sodb *db = arg1;
	so *e = so_of(&db->o);
	uint64_t lsn = sr_seq(e->r.seq, SR_LSN);
	if (t->vlsn == lsn)
		return SXPREPARE;
	sicache cache;
	si_cacheinit(&cache, &e->a_cursorcache);
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
	so *e = so_of(o);
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
	if (s == SXLOCK)
		return 2;
	if (s == SXROLLBACK) {
		so_objdestroy(&t->o);
		return 1;
	}
	assert(s == SXPREPARE);
	/* assign lsn */
	uint64_t lsn = 0;
	if (status == SO_RECOVER || e->ctl.commit_lsn)
		lsn = va_arg(args, uint64_t);
	sl_prepare(&e->lp, &t->t.log, lsn);
	return 0;
}

static int
so_txcommit(soobj *o, va_list args)
{
	sotx *t = (sotx*)o;
	so *e = so_of(o);
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

	/* log commit */
	if (status == SO_ONLINE) {
		sltx tl;
		sl_begin(&e->lp, &tl);
		rc = sl_write(&tl, &t->t.log);
		if (srunlikely(rc == -1)) {
			sl_rollback(&tl);
			so_objdestroy(&t->o);
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
so_txtype(soobj *o srunused, va_list args srunused) {
	return "transaction";
}

static soobjif sotxif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = so_txrollback,
	.error    = NULL,
	.set      = so_txset,
	.del      = so_txdelete,
	.drop     = so_txrollback,
	.get      = so_txget,
	.begin    = NULL,
	.prepare  = so_txprepare,
	.commit   = so_txcommit,
	.cursor   = NULL,
	.object   = NULL,
	.type     = so_txtype
};

soobj *so_txnew(so *e)
{
	sotx *t = sr_malloc(&e->a_tx, sizeof(sotx));
	if (srunlikely(t == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	so_objinit(&t->o, SOTX, &sotxif, &e->o);
	sx_begin(&e->xm, &t->t, 0);
	so_dbbind(e);
	so_objindex_register(&e->tx, &t->o);
	return &t->o;
}
