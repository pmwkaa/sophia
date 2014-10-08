
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
sliter_tx(stc *cx)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	sr_init(&r, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = cx->suite->logdir,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000
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
sliter_tx_read_empty(stc *cx)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	sr_init(&r, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = cx->suite->logdir,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000
	};
	slpool lp;
	t( sl_poolinit(&lp, &r, &conf) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	freelog(&log, &r);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &r);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (sr_iterhas(&li)) {
			sv *v = sr_iterof(&li);
			t( *(int*)svkey(v) == 7 );
			sr_iternext(&li);
		}
		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

static void
sliter_tx_read0(stc *cx)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	sr_init(&r, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = cx->suite->logdir,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000
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

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &r);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (sr_iterhas(&li)) {
			sv *v = sr_iterof(&li);
			t( *(int*)svkey(v) == 7 );
			sr_iternext(&li);
		}
		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

static void
sliter_tx_read1(stc *cx)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	sr_init(&r, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = cx->suite->logdir,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000
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
	freelog(&log, &r);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &r);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( sr_iterhas(&li) == 1 );
		sv *v = sr_iterof(&li);
		t( *(int*)svkey(v) == 7 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 8 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 9 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 0 );

		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

static void
sliter_tx_read2(stc *cx)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	sr_init(&r, &a, &seq, &cmp, NULL);
	slconf conf = {
		.dir        = cx->suite->logdir,
		.dir_read   = 1,
		.dir_write  = 1,
		.dir_create = 1,
		.rotatewm   = 1000
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

	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &r);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &r);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( sr_iterhas(&li) == 1 );
		sv *v = sr_iterof(&li);
		t( *(int*)svkey(v) == 7 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 8 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 9 );
		sr_iternext(&li);

		t( sr_iterhas(&li) == 0 );
		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

stgroup *sliter_group(void)
{
	stgroup *group = st_group("sliter");
	st_groupadd(group, st_test("tx", sliter_tx));
	st_groupadd(group, st_test("tx_read_empty", sliter_tx_read_empty));
	st_groupadd(group, st_test("tx_read0", sliter_tx_read0));
	st_groupadd(group, st_test("tx_read1", sliter_tx_read1));
	st_groupadd(group, st_test("tx_read2", sliter_tx_read2));
	return group;
}
