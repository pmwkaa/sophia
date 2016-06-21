
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
alloclogv(svlog *log, sr *r, uint8_t flags, int key)
{
	sfv pv[8];
	memset(pv, 0, sizeof(pv));
	pv[0].pointer = (char*)&key;
	pv[0].size = sizeof(uint32_t);
	pv[1].pointer = NULL;
	pv[1].size = 0;
	svv *v = sv_vbuild(r, pv);
	v->flags = flags;
	svlogv logv;
	logv.index_id = 0;
	logv.next = 0;
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
sl_itertx(void)
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
	sv_loginit_index(&log, 0, &st_r.r);

	alloclogv(&log, &st_r.r, 0, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );

	freelog(&log, &st_r.r);
	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_itertx_read_empty(void)
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
	sv_loginit_index(&log, 0, &st_r.r);
	freelog(&log, &st_r.r);

	sl *current = sscast(lp.list.prev, sl, link);
	ssiter li;
	ss_iterinit(sl_iter, &li);
	t( ss_iteropen(sl_iter, &li, &st_r.r, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (ss_iteratorhas(&li)) {
			sv *v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 7 );
			ss_iteratornext(&li);
		}
		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	ss_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_itertx_read0(void)
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
	sv_loginit_index(&log, 0, &st_r.r);
	alloclogv(&log, &st_r.r, 0, 7);
	sltx ltx;
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &st_r.r);

	sl *current = sscast(lp.list.prev, sl, link);
	ssiter li;
	ss_iterinit(sl_iter, &li);
	t( ss_iteropen(sl_iter, &li, &st_r.r, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (ss_iteratorhas(&li)) {
			sv *v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 7 );
			ss_iteratornext(&li);
		}
		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	ss_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_itertx_read1(void)
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
	sv_loginit_index(&log, 0, &st_r.r);
	alloclogv(&log, &st_r.r, 0, 7);
	alloclogv(&log, &st_r.r, 0, 8);
	alloclogv(&log, &st_r.r, 0, 9);
	sltx ltx;
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &st_r.r);

	sl *current = sscast(lp.list.prev, sl, link);
	ssiter li;
	ss_iterinit(sl_iter, &li);
	t( ss_iteropen(sl_iter, &li, &st_r.r, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( ss_iteratorhas(&li) == 1 );
		sv *v = ss_iteratorof(&li);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 7 );
		ss_iteratornext(&li);
		t( ss_iteratorhas(&li) == 1 );
		v = ss_iteratorof(&li);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 8 );
		ss_iteratornext(&li);
		t( ss_iteratorhas(&li) == 1 );
		v = ss_iteratorof(&li);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 9 );
		ss_iteratornext(&li);
		t( ss_iteratorhas(&li) == 0 );

		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	ss_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_itertx_read2(void)
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
	sv_loginit_index(&log, 0, &st_r.r);
	alloclogv(&log, &st_r.r, 0, 7);
	alloclogv(&log, &st_r.r, 0, 8);
	alloclogv(&log, &st_r.r, 0, 9);
	sltx ltx;
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );

	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &st_r.r);

	sl *current = sscast(lp.list.prev, sl, link);
	ssiter li;
	ss_iterinit(sl_iter, &li);
	t( ss_iteropen(sl_iter, &li, &st_r.r, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( ss_iteratorhas(&li) == 1 );
		sv *v = ss_iteratorof(&li);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 7 );
		ss_iteratornext(&li);
		t( ss_iteratorhas(&li) == 1 );
		v = ss_iteratorof(&li);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 8 );
		ss_iteratornext(&li);
		t( ss_iteratorhas(&li) == 1 );
		v = ss_iteratorof(&li);
		t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 9 );
		ss_iteratornext(&li);

		t( ss_iteratorhas(&li) == 0 );
		t( sl_iter_error(&li) == 0 );
		// commit
		if (! sl_iter_continue(&li) )
			break;
	}
	ss_iteratorclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
}

static void
sl_itertx_read3(void)
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
	sv_loginit_index(&log, 0, &st_r.r);
	alloclogv(&log, &st_r.r, 0, 7); /* single stmt */
	sltx ltx;
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &st_r.r);

	sv_loginit(&log, &st_r.a, 1);
	sv_loginit_index(&log, 0, &st_r.r);
	alloclogv(&log, &st_r.r, 0, 8); /* multi stmt */
	alloclogv(&log, &st_r.r, 0, 9);
	alloclogv(&log, &st_r.r, 0, 10);
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &st_r.r);

	sv_loginit(&log, &st_r.a, 1);
	sv_loginit_index(&log, 0, &st_r.r);
	alloclogv(&log, &st_r.r, 0, 11); /* multi stmt */
	alloclogv(&log, &st_r.r, 0, 12);
	alloclogv(&log, &st_r.r, 0, 13);
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &st_r.r);

	sv_loginit(&log, &st_r.a, 1);
	sv_loginit_index(&log, 0, &st_r.r);
	alloclogv(&log, &st_r.r, 0, 14); /* single stmt */
	t( sl_begin(&lp, &ltx, 0, 0) == 0 );
	t( sl_write(&ltx, &log) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &st_r.r);

	int state = 0;

	sl *current = sscast(lp.list.prev, sl, link);
	ssiter li;
	ss_iterinit(sl_iter, &li);
	t( ss_iteropen(sl_iter, &li, &st_r.r, &current->file, 1) == 0 );
	for (;;) {
		sv *v;
		// begin
		switch (state) {
		case 0:
			t( ss_iteratorhas(&li) == 1 );
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 7 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( v == NULL );
			break;
		case 1:
			t( ss_iteratorhas(&li) == 1 );
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 8 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 9 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 10 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( v == NULL );
			break;
		case 2:
			t( ss_iteratorhas(&li) == 1 );
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 11 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 12 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 13 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( v == NULL );
			break;
		case 3:
			t( ss_iteratorhas(&li) == 1 );
			v = ss_iteratorof(&li);
			t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 14 );
			ss_iteratornext(&li);
			v = ss_iteratorof(&li);
			t( v == NULL );
			break;
		}
		// commit
		if (! sl_iter_continue(&li) )
			break;
		state++;
	}
	ss_iteratorclose(&li);
	t( state == 3);

	t( sl_poolshutdown(&lp) == 0 );
}

stgroup *sl_iter_group(void)
{
	stgroup *group = st_group("sliter");
	st_groupadd(group, st_test("tx", sl_itertx));
	st_groupadd(group, st_test("tx_read_empty", sl_itertx_read_empty));
	st_groupadd(group, st_test("tx_read0", sl_itertx_read0));
	st_groupadd(group, st_test("tx_read1", sl_itertx_read1));
	st_groupadd(group, st_test("tx_read2", sl_itertx_read2));
	st_groupadd(group, st_test("tx_read3", sl_itertx_read3));
	return group;
}
