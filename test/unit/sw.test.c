
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
#include <libsw.h>
#include <libsd.h>
#include <libst.h>

static void
alloclogv(svlog *log, sr *r, uint64_t lsn, uint8_t flags, int key)
{
	sfv pv[8];
	memset(pv, 0, sizeof(pv));
	pv[0].pointer = (char*)&key;
	pv[0].size = sizeof(uint32_t);
	pv[1].pointer = NULL;
	pv[1].size = 0;
	svv *v = sv_vbuild(r, pv);
	sf_lsnset(r->scheme, sv_vpointer(v), lsn);
	sf_flagsset(r->scheme, sv_vpointer(v), flags);
	svlogv logv;
	logv.index_id = 0;
	logv.next = UINT32_MAX;
	logv.v = v;
	logv.ptr = NULL;
	sv_logadd(log, r, &logv);
}

static void
freelog(svlog *log, sr *c)
{
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &log->buf, sizeof(svlogv));
	for (; ss_iteratorhas(&i); ss_iteratornext(&i)) {
		svlogv *v = ss_iteratorof(&i);
		ss_free(c->a, v->v);
	}
	sv_logfree(log, c);
}

static void
sw_begin_commit(void)
{
	swmanager lp;
	t( sw_managerinit(&lp, &st_r.r) == 0 );
	swconf *conf = sw_conf(&lp);
	conf->path     = strdup(st_r.conf->log_dir);
	conf->enable   = 1;
	conf->rotatewm = 1000;
	t( sw_manageropen(&lp) == 0 );
	t( sw_managerrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log, &st_r.r, 1);
	sv_loginit_index(&log, 0, &st_r.r);

	alloclogv(&log, &st_r.r, 0, 0, 7);

	swtx ltx;
	t( sw_begin(&lp, &ltx, 0, 0) == 0 );
	t( sw_write(&ltx, &log) == 0 );
	t( sw_commit(&ltx) == 0 );

	freelog(&log, &st_r.r);
	t( sw_managershutdown(&lp) == 0 );
}

static void
sw_begin_rollback(void)
{
	swmanager lp;
	t( sw_managerinit(&lp, &st_r.r) == 0 );
	swconf *conf = sw_conf(&lp);
	conf->path     = strdup(st_r.conf->log_dir);
	conf->enable   = 1;
	conf->rotatewm = 1000;
	t( sw_manageropen(&lp) == 0 );
	t( sw_managerrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log, &st_r.r, 1);
	sv_loginit_index(&log, 0, &st_r.r);

	alloclogv(&log, &st_r.r, 0, 0, 7);

	swtx ltx;
	t( sw_begin(&lp, &ltx, 0, 0) == 0 );
	t( sw_write(&ltx, &log) == 0 );
	t( sw_rollback(&ltx) == 0 );

	freelog(&log, &st_r.r);
	t( sw_managershutdown(&lp) == 0 );
}

stgroup *sw_group(void)
{
	stgroup *group = st_group("sw");
	st_groupadd(group, st_test("begin_commit", sw_begin_commit));
	st_groupadd(group, st_test("begin_rollback", sw_begin_rollback));
	return group;
}
