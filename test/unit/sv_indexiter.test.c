
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
#include <libst.h>

static void
sv_indexiter_lte_empty(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, NULL, 0);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );
	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_eq(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 0, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 0, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 0, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(vb), vb->size);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(vc), vc->size);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lt_eq(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 0, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 0, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 0, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LT, sv_vpointer(va), va->size);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LT, sv_vpointer(vb), vb->size);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LT, sv_vpointer(vc), vc->size);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_empty(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 7);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_eq(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 0, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 0, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 0, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(vb), vb->size);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(vc), vc->size);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gt_eq(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 0, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 0, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 0, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GT, sv_vpointer(va), va->size);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GT, sv_vpointer(vb), vb->size);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GT, sv_vpointer(vc), vc->size);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_iterate0(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *h = st_svv(&st_r.g, NULL, 0, 0, keyb);
	t( sv_indexset(&i, &st_r.r, h) == 0 );
	svv *p = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, p) == 0 );

	svv *va = st_svv(&st_r.g, NULL, 1, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 3, 0, keya);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, NULL, 0);

	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == h );
	ss_iteratornext(&it);

	v = ss_iteratorof(&it);
	t( v->v == vc );
	ss_iteratornext(&it);

	v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	v = ss_iteratorof(&it);
	t( v->v == va );
	ss_iteratornext(&it);

	v = ss_iteratorof(&it);
	t( v->v == p );
	ss_iteratornext(&it);

	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_iterate1(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int j = 0;
	while (j < 16) {
		svv *v = st_svv(&st_r.g, NULL, j, 0, j);
		t( sv_indexset(&i, &st_r.r, v) == 0 );
		j++;
	}

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, NULL, 0);
	j = 0;
	while (ss_iteratorhas(&it)) {
		sv *v = ss_iteratorof(&it);
		t( sv_lsn(v) == j );
		ss_iteratornext(&it);
		j++;
	}
	t( j == 16 );

	sv_indexfree(&i, &st_r.r);
}

stgroup *sv_indexiter_group(void)
{
	stgroup *group = st_group("svindexiter");
	st_groupadd(group, st_test("lte_empty", sv_indexiter_lte_empty));
	st_groupadd(group, st_test("lte_eq", sv_indexiter_lte_eq));
	st_groupadd(group, st_test("lt_eq", sv_indexiter_lt_eq));
	st_groupadd(group, st_test("gte_empty", sv_indexiter_gte_empty));
	st_groupadd(group, st_test("gte_eq", sv_indexiter_gte_eq));
	st_groupadd(group, st_test("gt_eq", sv_indexiter_gt_eq));
	st_groupadd(group, st_test("iterate0", sv_indexiter_iterate0));
	st_groupadd(group, st_test("iterate1", sv_indexiter_iterate1));
	return group;
}
