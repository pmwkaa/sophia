
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
sl_begin_commit(stc *cx)
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

	alloclogv(&log, &r, 0, SVSET, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );

	freelog(&log, &r);
	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

static void
sl_begin_rollback(stc *cx)
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

	alloclogv(&log, &r, 0, SVSET, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_rollback(&ltx) == 0 );

	freelog(&log, &r);
	t( sl_poolshutdown(&lp) == 0 );
	sr_keyfree(&cmp, &a);
}

stgroup *sl_group(void)
{
	stgroup *group = st_group("sl");
	st_groupadd(group, st_test("begin_commit", sl_begin_commit));
	st_groupadd(group, st_test("begin_rollback", sl_begin_rollback));
	return group;
}
