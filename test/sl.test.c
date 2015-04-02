
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
	sv lv;
	sv_init(&lv, &sv_localif, &l, NULL);
	svv *v = sv_valloc(a, &lv);
	svlogv logv;
	logv.id = 0;
	logv.next = 0;
	sv_init(&logv.v, &sv_vif, v, NULL);
	sv_logadd(log, a, &logv, NULL);
}

static void
freelog(svlog *log, sr *c)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c);
	sr_iteropen(&i, &log->buf, sizeof(svlogv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		svlogv *v = sr_iterof(&i);
		sr_free(c->a, v->v.v);
	}
	sv_logfree(log, c->a);
}

static void
sl_begin_commit(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	srerror error;
	sr_errorinit(&error);
	srcrcf crc = sr_crc32c_function();
	sr_init(&r, &error, &a, &seq, &cmp, NULL, crc, NULL);
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
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srseq seq;
	sr_seqinit(&seq);
	sr r;
	srerror error;
	sr_errorinit(&error);
	srcrcf crc = sr_crc32c_function();
	sr_init(&r, &error, &a, &seq, &cmp, NULL, crc, NULL);
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

	alloclogv(&log, &a, 0, SVSET, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_rollback(&ltx) == 0 );

	freelog(&log, &r);
	t( sl_poolshutdown(&lp) == 0 );
}

stgroup *sl_group(void)
{
	stgroup *group = st_group("sl");
	st_groupadd(group, st_test("begin_commit", sl_begin_commit));
	st_groupadd(group, st_test("begin_rollback", sl_begin_rollback));
	return group;
}
