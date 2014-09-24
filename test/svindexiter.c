
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include "suite.h"

static svv*
allocv(sra *a, uint64_t lsn, uint8_t flags, int *key)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = key;
	l.keysize     = sizeof(int);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	return sv_valloc(a, &lv);
}

static void
test_lte_empty(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int key = 7;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lte_eq0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 0, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &r);
}

static void
test_lte_eq1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyb, sizeof(int), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
}

static void
test_lte_minmax(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lte_mid0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	key = 3;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	key = 6;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	key = 8;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sv_indexfree(&i, &r);
}

static void
test_lte_mid1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 15;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lte_iterate0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 15;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lte_iterate1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 15;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 3);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lte_iterate2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 2);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lt_eq(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 0, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LT, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LT, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LT, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lt_iterate(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LT, &keya, sizeof(keya), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_lte_dup_eq(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;

	svv *vold = NULL;
	svv *va = allocv(&a, 1, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 3, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sv_indexfree(&i, &r);
}

static void
test_lte_dup_mid(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *vold = NULL;
	svv *p = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, p, &vold) == 0 );
	p = allocv(&a, 4, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, p, &vold) == 0 );

	svv *va = allocv(&a, 1, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 3, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyc, sizeof(int), 5);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );

	sv_indexfree(&i, &r);
}

static void
test_lte_dup_iterate(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *vold = NULL;
	svv *h = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, h, &vold) == 0 );
	svv *p = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, p, &vold) == 0 );

	svv *va = allocv(&a, 1, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 3, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 20;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == p );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == h );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gte_empty(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int key = 7;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gte_eq0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 0, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &r);
}

static void
test_gte_eq1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyb, sizeof(int), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
}

static void
test_gte_minmax(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gte_mid0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	key = 3;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	key = 6;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	key = 8;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gte_mid1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gte_iterate0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 0;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gte_iterate1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 3);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gt_eq(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 0, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GT, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GT, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GT, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
}

static void
test_gt_iterate(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *vold = NULL;
	svv *va = allocv(&a, 4, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GT, &keyc, sizeof(keya), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_gte_dup_eq(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;

	svv *vold = NULL;
	svv *va = allocv(&a, 1, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 3, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sv_indexfree(&i, &r);
}

static void
test_gte_dup_mid(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *vold = NULL;
	svv *p = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, p, &vold) == 0 );
	p = allocv(&a, 4, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, p, &vold) == 0 );

	svv *va = allocv(&a, 1, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 3, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyc, sizeof(int), 5);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );

	sv_indexfree(&i, &r);
}

static void
test_gte_dup_iterate(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *vold = NULL;
	svv *h = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, h, &vold) == 0 );
	svv *p = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, p, &vold) == 0 );

	svv *va = allocv(&a, 1, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 3, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	int key = 2;
	sriter it;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == h );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_iterate_raw0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *vold = NULL;
	svv *h = allocv(&a, 0, SVSET, &keyb);
	t( sv_indexset(&i, &r, 0, h, &vold) == 0 );
	svv *p = allocv(&a, 2, SVSET, &keyc);
	t( sv_indexset(&i, &r, 0, p, &vold) == 0 );

	svv *va = allocv(&a, 1, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
	svv *vb = allocv(&a, 2, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vb, &vold) == 0 );
	svv *vc = allocv(&a, 3, SVSET, &keya);
	t( sv_indexset(&i, &r, 0, vc, &vold) == 0 );

	sriter it;
	sr_iterinit(&it, &sv_indexiterraw, &r);
	sr_iteropen(&it, &i);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == h );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == p );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
test_iterate_raw1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	svv *vold = NULL;
	int j = 0;
	while (j < 16) {
		svv *v = allocv(&a, j, SVSET, &j);
		t( sv_indexset(&i, &r, 0, v, &vold) == 0 );
		t( vold == NULL );
		j++;
	}

	sriter it;
	sr_iterinit(&it, &sv_indexiterraw, &r);
	sr_iteropen(&it, &i);
	j = 0;
	while (sr_iterhas(&it)) {
		sv *v = sr_iterof(&it);
		t( svlsn(v) == j );
		sr_iternext(&it);
		j++;
	}
	t( j == 16 );

	sv_indexfree(&i, &r);
}

int
main(int argc, char *argv[])
{
	test( test_lte_empty );
	test( test_lte_eq0 );
	test( test_lte_eq1 );
	test( test_lte_minmax );
	test( test_lte_mid0 );
	test( test_lte_mid1 );
	test( test_lte_iterate0 );
	test( test_lte_iterate1 );
	test( test_lte_iterate2 );
	test( test_lt_eq );
	test( test_lt_iterate );
	test( test_lte_dup_eq );
	test( test_lte_dup_mid );
	test( test_lte_dup_iterate );

	test( test_gte_empty );
	test( test_gte_eq0 );
	test( test_gte_eq1 );
	test( test_gte_minmax );
	test( test_gte_mid0 );
	test( test_gte_mid1 );
	test( test_gte_iterate0 );
	test( test_gte_iterate1 );
	test( test_gt_eq );
	test( test_gt_iterate );
	test( test_gte_dup_eq );
	test( test_gte_dup_mid );
	test( test_gte_dup_iterate );

	test( test_iterate_raw0 );
	test( test_iterate_raw1 );
	return 0;
}
