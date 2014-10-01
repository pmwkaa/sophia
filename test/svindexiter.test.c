
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libst.h>
#include <sophia.h>

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
svindexiter_lte_empty(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(int), 0ULL);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_eq0(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyb, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyc, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_eq1(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1ULL);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyb, sizeof(int), 8ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_minmax(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 8ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 3ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 1ULL);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_mid0(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	key = 3;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	key = 6;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	key = 8;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_mid1(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 3ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 1ULL);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_iterate0(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8ULL);
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
svindexiter_lte_iterate1(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 3ULL);

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
svindexiter_lte_iterate2(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 2ULL);

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
svindexiter_lt_eq(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LT, &keya, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LT, &keyb, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LT, &keyc, sizeof(int), 0ULL);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lt_iterate(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LT, &keya, sizeof(keya), 8ULL);
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
svindexiter_lte_dup_eq(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 3ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_dup_mid(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 3ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_LTE, &keyc, sizeof(int), 5ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );

	sv_indexfree(&i, &r);
}

static void
svindexiter_lte_dup_iterate(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(int), 2ULL);
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
svindexiter_gte_empty(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(int), 0ULL);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_eq0(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyb, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyc, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_eq1(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1ULL);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyb, sizeof(int), 8ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_minmax(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 8ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 3ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 1ULL);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_mid0(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	key = 3;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	key = 6;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	key = 8;
	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8ULL);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_mid1(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 3ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 1ULL);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_iterate0(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8ULL);
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
svindexiter_gte_iterate1(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 3ULL);

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
svindexiter_gt_eq(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GT, &keya, sizeof(int), 0ULL);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GT, &keyb, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GT, &keyc, sizeof(int), 0ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gt_iterate(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GT, &keyc, sizeof(keya), 8ULL);
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
svindexiter_gte_dup_eq(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 3ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_dup_mid(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 3ULL);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 2ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sv_indexiter, &r);
	sr_iteropen(&it, &i, SR_GTE, &keyc, sizeof(int), 5ULL);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );

	sv_indexfree(&i, &r);
}

static void
svindexiter_gte_dup_iterate(stc *c srunused)
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
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(int), 2ULL);
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
svindexiter_random(stc *c srunused)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcomparator cmp = { sr_cmpu32, NULL };
	sr r;
	sr_init(&r, &a, NULL, &cmp);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int key = 0;
	for (; key < 100; key++) {
		svv *vold = NULL;
		svv *va = allocv(&a, key, SVSET, &key);
		t( sv_indexset(&i, &r, 0, va, &vold) == 0 );
		t( vold == NULL );
	}
	srand(54321);
	key = 0;
	for (; key < 1000; key++) {
		uint32_t rnd = rand() % 100;
		sriter it;
		sr_iterinit(&it, &sv_indexiter, &r);
		sr_iteropen(&it, &i, SR_RANDOM, &rnd, sizeof(rnd), UINT64_MAX);
		t( sr_iterhas(&it) != 0 );
		sv *v = sr_iterof(&it);
		int k = *(int*)svkey(v);
		t( k >= 0 && k < 100 );
	}
	sv_indexfree(&i, &r);
}


static void
svindexiter_iterate_raw0(stc *c srunused)
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
svindexiter_iterate_raw1(stc *c srunused)
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

st *svindexiter_group(void)
{
	st *group = st_def("svindexiter", NULL);
	st_test(group, st_def("lte_empty", svindexiter_lte_empty));
	st_test(group, st_def("lte_eq0", svindexiter_lte_eq0));
	st_test(group, st_def("lte_eq1", svindexiter_lte_eq1));
	st_test(group, st_def("lte_minmax", svindexiter_lte_minmax));
	st_test(group, st_def("lte_mid0", svindexiter_lte_mid0));
	st_test(group, st_def("lte_mid1", svindexiter_lte_mid1));
	st_test(group, st_def("lte_iterate0", svindexiter_lte_iterate0));
	st_test(group, st_def("lte_iterate1", svindexiter_lte_iterate1));
	st_test(group, st_def("lte_iterate2", svindexiter_lte_iterate2));
	st_test(group, st_def("lt_eq", svindexiter_lt_eq));
	st_test(group, st_def("lt_iterate", svindexiter_lt_iterate));
	st_test(group, st_def("lte_dup_eq", svindexiter_lte_dup_eq));
	st_test(group, st_def("lte_dup_mid", svindexiter_lte_dup_mid));
	st_test(group, st_def("lte_dup_iterate", svindexiter_lte_dup_iterate));
	st_test(group, st_def("gte_empty", svindexiter_gte_empty));
	st_test(group, st_def("gte_eq0", svindexiter_gte_eq0));
	st_test(group, st_def("gte_eq1", svindexiter_gte_eq1));
	st_test(group, st_def("gte_minmax", svindexiter_gte_minmax));
	st_test(group, st_def("gte_mid0", svindexiter_gte_mid0));
	st_test(group, st_def("gte_mid1", svindexiter_gte_mid1));
	st_test(group, st_def("gte_iterate0", svindexiter_gte_iterate0));
	st_test(group, st_def("gte_iterate1", svindexiter_gte_iterate1));
	st_test(group, st_def("gt_eq", svindexiter_gt_eq));
	st_test(group, st_def("gt_iterate", svindexiter_gt_iterate));
	st_test(group, st_def("gte_dup_eq", svindexiter_gte_dup_eq));
	st_test(group, st_def("gte_dup_mid", svindexiter_gte_dup_mid));
	st_test(group, st_def("gte_dup_iterate", svindexiter_gte_dup_iterate));
	st_test(group, st_def("random", svindexiter_random));
	st_test(group, st_def("iterate_raw0", svindexiter_iterate_raw0));
	st_test(group, st_def("iterate_raw1", svindexiter_iterate_raw1));
	return group;
}
