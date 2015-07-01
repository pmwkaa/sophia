
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
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, NULL, 0, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );
	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_eq0(void)
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
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_eq1(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 4, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(vb), vb->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_minmax(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, NULL, 0, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, NULL, 0, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, NULL, 0, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_mid0(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	key = st_svv(&st_r.g, &st_r.gc, 0, 0, 3);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	key = st_svv(&st_r.g, &st_r.gc, 0, 0, 6);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	key = st_svv(&st_r.g, &st_r.gc, 0, 0, 8);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_mid1(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 15);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_iterate0(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 15);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_iterate1(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 15);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 3ULL);

	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_iterate2(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, NULL, 0, 2ULL);

	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

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
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LT, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LT, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LT, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lt_iterate(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LT, sv_vpointer(va), va->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_dup_eq(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;

	svv *va = st_svv(&st_r.g, NULL, 1, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 3, 0, keya);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_dup_mid(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *p = st_svv(&st_r.g, NULL, 0, 0, keyb);
	t( sv_indexset(&i, &st_r.r, p) == 0 );
	p = st_svv(&st_r.g, NULL, 4, 0, keyc);
	t( sv_indexset(&i, &st_r.r, p) == 0 );

	svv *va = st_svv(&st_r.g, NULL, 1, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 3, 0, keya);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, keyc);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 5ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == p );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_lte_dup_iterate(void)
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

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 20);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_LTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == p );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == h );
	ss_iteratornext(&it);

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
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_eq0(void)
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
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_eq1(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 4, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(vb), vb->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_minmax(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 4, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, NULL, 0, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, NULL, 0, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, NULL, 0, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_mid0(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	key = st_svv(&st_r.g, &st_r.gc, 0, 0, 3);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	key = st_svv(&st_r.g, &st_r.gc, 0, 0, 6);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	key = st_svv(&st_r.g, &st_r.gc, 0, 0, 8);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_mid1(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 4, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_iterate0(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 0);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_iterate1(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 4, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 3ULL);

	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

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
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GT, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GT, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GT, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gt_iterate(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = st_svv(&st_r.g, NULL, 4, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 3, 0, keyb);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 2, 0, keyc);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GT, sv_vpointer(vc), vc->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_dup_eq(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;

	svv *va = st_svv(&st_r.g, NULL, 1, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 3, 0, keya);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );
	/**/
	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_dup_mid(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *p = st_svv(&st_r.g, NULL, 0, 0, keyb);
	t( sv_indexset(&i, &st_r.r, p) == 0 );
	p = st_svv(&st_r.g, NULL, 4, 0, keyc);
	t( sv_indexset(&i, &st_r.r, p) == 0 );

	svv *va = st_svv(&st_r.g, NULL, 1, 0, keya);
	t( sv_indexset(&i, &st_r.r, va) == 0 );
	svv *vb = st_svv(&st_r.g, NULL, 2, 0, keya);
	t( sv_indexset(&i, &st_r.r, vb) == 0 );
	svv *vc = st_svv(&st_r.g, NULL, 3, 0, keya);
	t( sv_indexset(&i, &st_r.r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, keyc);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 5ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == p );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_gte_dup_iterate(void)
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

	svv *key = st_svv(&st_r.g, &st_r.gc, 0, 0, 2);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &st_r.r, &i, SS_GTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == h );
	ss_iteratornext(&it);

	v = ss_iteratorof(&it);
	t( v->v == vb );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == p );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &st_r.r);
}

static void
sv_indexiter_iterate_raw0(void)
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
	ss_iterinit(sv_indexiterraw, &it);
	ss_iteropen(sv_indexiterraw, &it, &i);

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
sv_indexiter_iterate_raw1(void)
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
	ss_iterinit(sv_indexiterraw, &it);
	ss_iteropen(sv_indexiterraw, &it, &i);
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
	st_groupadd(group, st_test("lte_eq0", sv_indexiter_lte_eq0));
	st_groupadd(group, st_test("lte_eq1", sv_indexiter_lte_eq1));
	st_groupadd(group, st_test("lte_minmax", sv_indexiter_lte_minmax));
	st_groupadd(group, st_test("lte_mid0", sv_indexiter_lte_mid0));
	st_groupadd(group, st_test("lte_mid1", sv_indexiter_lte_mid1));
	st_groupadd(group, st_test("lte_iterate0", sv_indexiter_lte_iterate0));
	st_groupadd(group, st_test("lte_iterate1", sv_indexiter_lte_iterate1));
	st_groupadd(group, st_test("lte_iterate2", sv_indexiter_lte_iterate2));
	st_groupadd(group, st_test("lt_eq", sv_indexiter_lt_eq));
	st_groupadd(group, st_test("lt_iterate", sv_indexiter_lt_iterate));
	st_groupadd(group, st_test("lte_dup_eq", sv_indexiter_lte_dup_eq));
	st_groupadd(group, st_test("lte_dup_mid", sv_indexiter_lte_dup_mid));
	st_groupadd(group, st_test("lte_dup_iterate", sv_indexiter_lte_dup_iterate));
	st_groupadd(group, st_test("gte_empty", sv_indexiter_gte_empty));
	st_groupadd(group, st_test("gte_eq0", sv_indexiter_gte_eq0));
	st_groupadd(group, st_test("gte_eq1", sv_indexiter_gte_eq1));
	st_groupadd(group, st_test("gte_minmax", sv_indexiter_gte_minmax));
	st_groupadd(group, st_test("gte_mid0", sv_indexiter_gte_mid0));
	st_groupadd(group, st_test("gte_mid1", sv_indexiter_gte_mid1));
	st_groupadd(group, st_test("gte_iterate0", sv_indexiter_gte_iterate0));
	st_groupadd(group, st_test("gte_iterate1", sv_indexiter_gte_iterate1));
	st_groupadd(group, st_test("gt_eq", sv_indexiter_gt_eq));
	st_groupadd(group, st_test("gt_iterate", sv_indexiter_gt_iterate));
	st_groupadd(group, st_test("gte_dup_eq", sv_indexiter_gte_dup_eq));
	st_groupadd(group, st_test("gte_dup_mid", sv_indexiter_gte_dup_mid));
	st_groupadd(group, st_test("gte_dup_iterate", sv_indexiter_gte_dup_iterate));
	st_groupadd(group, st_test("iterate_raw0", sv_indexiter_iterate_raw0));
	st_groupadd(group, st_test("iterate_raw1", sv_indexiter_iterate_raw1));
	return group;
}
