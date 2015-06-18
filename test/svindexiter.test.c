
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libst.h>
#include <sophia.h>

static svv*
allocv(sr *r, uint64_t lsn, uint8_t flags, uint32_t key)
{
	sfv pv;
	pv.key = (char*)&key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	svv *v = sv_vbuild(r, &pv, 1, NULL, 0);
	v->lsn = lsn;
	v->flags = flags;
	return v;
}

static void
svindexiter_lte_empty(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, NULL, 0, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );
	sv_indexfree(&i, &r);
	ss_aclose(&a);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_eq0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 0, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 0, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_eq1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 4, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(vb), vb->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_minmax(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, NULL, 0, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, NULL, 0, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, NULL, 0, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_mid0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 0, 0, 3);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );
	sv_vfree(&a, key);

	key = allocv(&r, 0, 0, 6);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );
	sv_vfree(&a, key);

	key = allocv(&r, 0, 0, 8);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );
	sv_vfree(&a, key);

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_mid1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 15);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 15);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 8ULL);
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

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 15);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 3ULL);

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

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_iterate2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, NULL, 0, 2ULL);

	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lt_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 0, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 0, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LT, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LT, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LT, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lt_iterate(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LT, sv_vpointer(va), va->size, 8ULL);
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

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_dup_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;

	svv *va = allocv(&r, 1, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 3, 0, keya);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_dup_mid(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *p = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, p) == 0 );
	p = allocv(&r, 4, 0, keyc);
	t( sv_indexset(&i, &r, p) == 0 );

	svv *va = allocv(&r, 1, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 3, 0, keya);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	svv *key = allocv(&r, 0, 0, keyc);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 5ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == p );
	sv_vfree(&a, key);

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_lte_dup_iterate(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *h = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, h) == 0 );
	svv *p = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, p) == 0 );

	svv *va = allocv(&r, 1, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 3, 0, keya);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 20);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_LTE, sv_vpointer(key), key->size, 2ULL);
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

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_empty(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	svv *key = allocv(&r, 0, 0, 7);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_eq0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 0, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 0, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vc );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_eq1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 4, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(vb), vb->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_minmax(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 4, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, NULL, 0, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, NULL, 0, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, NULL, 0, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_mid0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );
	sv_vfree(&a, key);

	key = allocv(&r, 0, 0, 3);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );
	sv_vfree(&a, key);

	key = allocv(&r, 0, 0, 6);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );
	sv_vfree(&a, key);

	key = allocv(&r, 0, 0, 8);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_mid1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 4, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 0);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 8ULL);
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

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 4, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 1);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 3ULL);

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

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gt_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 0, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 0, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GT, sv_vpointer(va), va->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GT, sv_vpointer(vb), vb->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GT, sv_vpointer(vc), vc->size, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gt_iterate(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	svv *va = allocv(&r, 4, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 3, 0, keyb);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GT, sv_vpointer(vc), vc->size, 8ULL);
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

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_dup_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keya = 7;

	svv *va = allocv(&r, 1, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 3, 0, keya);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );
	/**/
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_dup_mid(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *p = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, p) == 0 );
	p = allocv(&r, 4, 0, keyc);
	t( sv_indexset(&i, &r, p) == 0 );

	svv *va = allocv(&r, 1, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 3, 0, keya);
	t( sv_indexset(&i, &r, vc) == 0 );

	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v->v == vc );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == vb );

	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(va), va->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == va );

	svv *key = allocv(&r, 0, 0, keyc);
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 5ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( v->v == p );
	sv_vfree(&a, key);

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_gte_dup_iterate(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *h = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, h) == 0 );
	svv *p = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, p) == 0 );

	svv *va = allocv(&r, 1, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 3, 0, keya);
	t( sv_indexset(&i, &r, vc) == 0 );

	svv *key = allocv(&r, 0, 0, 2);
	ssiter it;
	ss_iterinit(sv_indexiter, &it);
	ss_iteropen(sv_indexiter, &it, &r, &i, SS_GTE, sv_vpointer(key), key->size, 2ULL);
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

	sv_vfree(&a, key);
	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_iterate_raw0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	svv *h = allocv(&r, 0, 0, keyb);
	t( sv_indexset(&i, &r, h) == 0 );
	svv *p = allocv(&r, 2, 0, keyc);
	t( sv_indexset(&i, &r, p) == 0 );

	svv *va = allocv(&r, 1, 0, keya);
	t( sv_indexset(&i, &r, va) == 0 );
	svv *vb = allocv(&r, 2, 0, keya);
	t( sv_indexset(&i, &r, vb) == 0 );
	svv *vc = allocv(&r, 3, 0, keya);
	t( sv_indexset(&i, &r, vc) == 0 );

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

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

static void
svindexiter_iterate_raw1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, SF_KV, SF_SRAW, NULL, &cmp, NULL, NULL, NULL);

	svindex i;
	t( sv_indexinit(&i) == 0 );

	int j = 0;
	while (j < 16) {
		svv *v = allocv(&r, j, 0, j);
		t( sv_indexset(&i, &r, v) == 0 );
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

	sv_indexfree(&i, &r);
	sr_schemefree(&cmp, &a);
}

stgroup *svindexiter_group(void)
{
	stgroup *group = st_group("svindexiter");
	st_groupadd(group, st_test("lte_empty", svindexiter_lte_empty));
	st_groupadd(group, st_test("lte_eq0", svindexiter_lte_eq0));
	st_groupadd(group, st_test("lte_eq1", svindexiter_lte_eq1));
	st_groupadd(group, st_test("lte_minmax", svindexiter_lte_minmax));
	st_groupadd(group, st_test("lte_mid0", svindexiter_lte_mid0));
	st_groupadd(group, st_test("lte_mid1", svindexiter_lte_mid1));
	st_groupadd(group, st_test("lte_iterate0", svindexiter_lte_iterate0));
	st_groupadd(group, st_test("lte_iterate1", svindexiter_lte_iterate1));
	st_groupadd(group, st_test("lte_iterate2", svindexiter_lte_iterate2));
	st_groupadd(group, st_test("lt_eq", svindexiter_lt_eq));
	st_groupadd(group, st_test("lt_iterate", svindexiter_lt_iterate));
	st_groupadd(group, st_test("lte_dup_eq", svindexiter_lte_dup_eq));
	st_groupadd(group, st_test("lte_dup_mid", svindexiter_lte_dup_mid));
	st_groupadd(group, st_test("lte_dup_iterate", svindexiter_lte_dup_iterate));
	st_groupadd(group, st_test("gte_empty", svindexiter_gte_empty));
	st_groupadd(group, st_test("gte_eq0", svindexiter_gte_eq0));
	st_groupadd(group, st_test("gte_eq1", svindexiter_gte_eq1));
	st_groupadd(group, st_test("gte_minmax", svindexiter_gte_minmax));
	st_groupadd(group, st_test("gte_mid0", svindexiter_gte_mid0));
	st_groupadd(group, st_test("gte_mid1", svindexiter_gte_mid1));
	st_groupadd(group, st_test("gte_iterate0", svindexiter_gte_iterate0));
	st_groupadd(group, st_test("gte_iterate1", svindexiter_gte_iterate1));
	st_groupadd(group, st_test("gt_eq", svindexiter_gt_eq));
	st_groupadd(group, st_test("gt_iterate", svindexiter_gt_iterate));
	st_groupadd(group, st_test("gte_dup_eq", svindexiter_gte_dup_eq));
	st_groupadd(group, st_test("gte_dup_mid", svindexiter_gte_dup_mid));
	st_groupadd(group, st_test("gte_dup_iterate", svindexiter_gte_dup_iterate));
	st_groupadd(group, st_test("iterate_raw0", svindexiter_iterate_raw0));
	st_groupadd(group, st_test("iterate_raw1", svindexiter_iterate_raw1));
	return group;
}
