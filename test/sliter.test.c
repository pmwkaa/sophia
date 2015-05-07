
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
alloclogv(svlog *log, sr *r, uint64_t lsn, uint8_t flags, int key)
{
	srformatv pv;
	pv.key = (char*)&key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	svv *v = sv_vbuild(r, &pv, 1, NULL, 0);
	v->lsn = lsn;
	v->flags = flags;
	svlogv logv;
	logv.id = 0;
	logv.next = 0;
	sv_init(&logv.v, &sv_vif, v, NULL);
	sv_logadd(log, r->a, &logv, NULL);
}

static void
freelog(svlog *log, sr *c)
{
	sriter i;
	sr_iterinit(sr_bufiter, &i, c);
	sr_iteropen(sr_bufiter, &i, &log->buf, sizeof(svlogv));
	for (; sr_iteratorhas(&i); sr_iteratornext(&i)) {
		svlogv *v = sr_iteratorof(&i);
		sr_free(c->a, v->v.v);
	}
	sv_logfree(log, c->a);
}

static void
sliter_tx(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, NULL, crc, NULL);

	slconf conf = {
		.path     = cx->suite->logdir,
		.enable   = 1,
		.rotatewm = 1000
	};
	slpool lp;
	t( sl_poolinit(&lp, &r) == 0 );
	t( sl_poolopen(&lp, &conf) == 0 );
	t( sl_poolrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log);

	alloclogv(&log, &r, 0, 0, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );

	freelog(&log, &r);
	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

static void
sliter_tx_read_empty(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, NULL, crc, NULL);

	slconf conf = {
		.path     = cx->suite->logdir,
		.enable   = 1,
		.rotatewm = 1000
	};
	slpool lp;
	t( sl_poolinit(&lp, &r) == 0 );
	t( sl_poolopen(&lp, &conf) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	freelog(&log, &r);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(sl_iter, &li, &r);
	t( sr_iteropen(sl_iter, &li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (sr_iteratorhas(&li)) {
			sv *v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 7 );
			sr_iteratornext(&li);
		}
		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	sr_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

static void
sliter_tx_read0(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, NULL, crc, NULL);

	slconf conf = {
		.path     = cx->suite->logdir,
		.enable   = 1,
		.rotatewm = 1000
	};
	slpool lp;
	t( sl_poolinit(&lp, &r) == 0 );
	t( sl_poolopen(&lp, &conf) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	alloclogv(&log, &r, 0, 0, 7);
	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &r);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(sl_iter, &li, &r);
	t( sr_iteropen(sl_iter, &li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (sr_iteratorhas(&li)) {
			sv *v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 7 );
			sr_iteratornext(&li);
		}
		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	sr_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

static void
sliter_tx_read1(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, NULL, crc, NULL);

	slconf conf = {
		.path     = cx->suite->logdir,
		.enable   = 1,
		.rotatewm = 1000
	};
	slpool lp;
	t( sl_poolinit(&lp, &r) == 0 );
	t( sl_poolopen(&lp, &conf) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	alloclogv(&log, &r, 0, 0, 7);
	alloclogv(&log, &r, 0, 0, 8);
	alloclogv(&log, &r, 0, 0, 9);
	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &r);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(sl_iter, &li, &r);
	t( sr_iteropen(sl_iter, &li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( sr_iteratorhas(&li) == 1 );
		sv *v = sr_iteratorof(&li);
		t( *(int*)sv_key(v, &r, 0) == 7 );
		sr_iteratornext(&li);
		t( sr_iteratorhas(&li) == 1 );
		v = sr_iteratorof(&li);
		t( *(int*)sv_key(v, &r, 0) == 8 );
		sr_iteratornext(&li);
		t( sr_iteratorhas(&li) == 1 );
		v = sr_iteratorof(&li);
		t( *(int*)sv_key(v, &r, 0) == 9 );
		sr_iteratornext(&li);
		t( sr_iteratorhas(&li) == 0 );

		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	sr_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

static void
sliter_tx_read2(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, NULL, crc, NULL);

	slconf conf = {
		.path     = cx->suite->logdir,
		.enable   = 1,
		.rotatewm = 1000
	};
	slpool lp;
	t( sl_poolinit(&lp, &r) == 0 );
	t( sl_poolopen(&lp, &conf) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	alloclogv(&log, &r, 0, 0, 7);
	alloclogv(&log, &r, 0, 0, 8);
	alloclogv(&log, &r, 0, 0, 9);
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
	sr_iterinit(sl_iter, &li, &r);
	t( sr_iteropen(sl_iter, &li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( sr_iteratorhas(&li) == 1 );
		sv *v = sr_iteratorof(&li);
		t( *(int*)sv_key(v, &r, 0) == 7 );
		sr_iteratornext(&li);
		t( sr_iteratorhas(&li) == 1 );
		v = sr_iteratorof(&li);
		t( *(int*)sv_key(v, &r, 0) == 8 );
		sr_iteratornext(&li);
		t( sr_iteratorhas(&li) == 1 );
		v = sr_iteratorof(&li);
		t( *(int*)sv_key(v, &r, 0) == 9 );
		sr_iteratornext(&li);

		t( sr_iteratorhas(&li) == 0 );
		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	sr_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

static void
sliter_tx_read3(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, NULL, crc, NULL);

	slconf conf = {
		.path     = cx->suite->logdir,
		.enable   = 1,
		.rotatewm = 1000
	};
	slpool lp;
	t( sl_poolinit(&lp, &r) == 0 );
	t( sl_poolopen(&lp, &conf) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;

	sv_loginit(&log);
	alloclogv(&log, &r, 0, 0, 7); /* single stmt */
	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &r);

	sv_loginit(&log);
	alloclogv(&log, &r, 0, 0, 8); /* multi stmt */
	alloclogv(&log, &r, 0, 0, 9);
	alloclogv(&log, &r, 0, 0, 10);
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &r);

	sv_loginit(&log);
	alloclogv(&log, &r, 0, 0, 11); /* multi stmt */
	alloclogv(&log, &r, 0, 0, 12);
	alloclogv(&log, &r, 0, 0, 13);
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &r);

	sv_loginit(&log);
	alloclogv(&log, &r, 0, 0, 14); /* single stmt */
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &r);

	int state = 0;

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(sl_iter, &li, &r);
	t( sr_iteropen(sl_iter, &li, &current->file, 1) == 0 );
	for (;;) {
		sv *v;
		// begin
		switch (state) {
		case 0:
			t( sr_iteratorhas(&li) == 1 );
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 7 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( v == NULL );
			break;
		case 1:
			t( sr_iteratorhas(&li) == 1 );
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 8 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 9 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 10 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( v == NULL );
			break;
		case 2:
			t( sr_iteratorhas(&li) == 1 );
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 11 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 12 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 13 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( v == NULL );
			break;
		case 3:
			t( sr_iteratorhas(&li) == 1 );
			v = sr_iteratorof(&li);
			t( *(int*)sv_key(v, &r, 0) == 14 );
			sr_iteratornext(&li);
			v = sr_iteratorof(&li);
			t( v == NULL );
			break;
		}
		// commit
		if (! sl_iter_continue(&li) )
			break;
		state++;
	}
	sr_iteratorclose(&li);
	t( state == 3);

	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

stgroup *sliter_group(void)
{
	stgroup *group = st_group("sliter");
	st_groupadd(group, st_test("tx", sliter_tx));
	st_groupadd(group, st_test("tx_read_empty", sliter_tx_read_empty));
	st_groupadd(group, st_test("tx_read0", sliter_tx_read0));
	st_groupadd(group, st_test("tx_read1", sliter_tx_read1));
	st_groupadd(group, st_test("tx_read2", sliter_tx_read2));
	st_groupadd(group, st_test("tx_read3", sliter_tx_read3));
	return group;
}
