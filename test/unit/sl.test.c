
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libst.h>

static void
alloclogv(svlog *log, sr *r, uint64_t lsn, uint8_t flags, int key)
{
	sfv pv[2];
	pv[0].pointer = (char*)&key;
	pv[0].size = sizeof(uint32_t);
	pv[1].pointer = NULL;
	pv[1].size = 0;
	svv *v = sv_vbuild(r, pv);
	v->lsn = lsn;
	v->flags = flags;
	svlogv logv;
	logv.index_id = 0;
	logv.next = UINT32_MAX;
	sv_init(&logv.v, &sv_vif, v, NULL);
	sv_logadd(log, r->a, &logv);
}

static void
freelog(svlog *log, sr *c)
{
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &log->buf, sizeof(svlogv));
	for (; ss_iteratorhas(&i); ss_iteratornext(&i)) {
		svlogv *v = ss_iteratorof(&i);
		ss_free(c->a, v->v.v);
	}
	sv_logfree(log, c->a);
}

static void
sl_begin_commit(void)
{
	slpool lp;
	t( sl_poolinit(&lp, &st_r.r) == 0 );
	slconf *conf = sl_conf(&lp);
	conf->path     = strdup(st_r.conf->log_dir);
	conf->enable   = 1;
	conf->rotatewm = 1000;
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log, &st_r.a, 1);

	alloclogv(&log, &st_r.r, 0, 0, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );

	freelog(&log, &st_r.r);
	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_begin_rollback(void)
{
	slpool lp;
	t( sl_poolinit(&lp, &st_r.r) == 0 );
	slconf *conf = sl_conf(&lp);
	conf->path     = strdup(st_r.conf->log_dir);
	conf->enable   = 1;
	conf->rotatewm = 1000;
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log, &st_r.a, 1);

	alloclogv(&log, &st_r.r, 0, 0, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_rollback(&ltx) == 0 );

	freelog(&log, &st_r.r);
	t( sl_poolshutdown(&lp) == 0 );
}

stgroup *sl_group(void)
{
	stgroup *group = st_group("sl");
	st_groupadd(group, st_test("begin_commit", sl_begin_commit));
	st_groupadd(group, st_test("begin_rollback", sl_begin_rollback));
	return group;
}
