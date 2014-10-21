
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libst.h>
#include <sophia.h>

static void
alloclogv(svlog *log, sra *a, uint64_t lsn, uint8_t flags, int key)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = &key;
	l.keysize     = sizeof(int);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	svv *v = sv_valloc(a, &lv);
	sv vv;
	svinit(&vv, &sv_vif, v, NULL);
	sv_logadd(log, a, &vv);
}

static void
freelog(svlog *log, sr *c)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c);
	sr_iteropen(&i, &log->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		sr_free(c->a, v->v);
	}
	sv_logfree(log, c->a);
}

static void
sl_begin_commit(stc *cx)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = cx->suite->logdir,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000,
		.expand     = 0
	};
	slpool lp;
	t( sl_poolinit(&lp, &r, &conf) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log);

	alloclogv(&log, &a, 0, SVSET, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );

	freelog(&log, &r);
	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_begin_rollback(stc *cx)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = cx->suite->logdir,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000,
		.expand     = 0
	};
	slpool lp;
	t( sl_poolinit(&lp, &r, &conf) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log);

	alloclogv(&log, &a, 0, SVSET, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_rollback(&ltx) == 0 );

	freelog(&log, &r);
	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_expand(stc *cx)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = NULL,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000,
		.expand     = 1
	};
	slpool lp;
	t( sl_poolinit(&lp, &r, &conf) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log);

	alloclogv(&log, &a, 0, SVSET, 7);
	alloclogv(&log, &a, 0, SVSET, 8);
	alloclogv(&log, &a, 0, SVSET, 9);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );

	uint64_t lsn = 0;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, &r);
	sr_iteropen(&i, &log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		t( svlsn(v) == lsn );
		lsn++;
	}
	freelog(&log, &r);
	t( sl_poolshutdown(&lp) == 0 );
}


stgroup *sl_group(void)
{
	stgroup *group = st_group("sl");
	st_groupadd(group, st_test("begin_commit", sl_begin_commit));
	st_groupadd(group, st_test("begin_rollback", sl_begin_rollback));
	st_groupadd(group, st_test("expand", sl_expand));
	return group;
}
