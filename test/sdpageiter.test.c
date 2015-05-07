
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libst.h>
#include <sophia.h>

static void*
allocv(sr *r, int key)
{
	srformatv pv;
	pv.key = (char*)&key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	return sv_vbuild(r, &pv, 1, NULL, 0);
}

static void
addv(sdbuild *b, sr *r, uint64_t lsn, uint8_t flags, int *key)
{
	srformatv pv;
	pv.key = (char*)key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	svv *v = sv_vbuild(r, &pv, 1, NULL, 0);
	v->lsn = lsn;
	v->flags = flags;
	sv vv;
	sv_init(&vv, &sv_vif, v, NULL);
	sd_buildadd(b, r, &vv, flags & SVDUP);
	sv_vfree(r->a, v);
}

static void
sdpageiter_lte_empty(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	sv *v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_eq0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_eq1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_eq2(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	sv *v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_minmax0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	t( sr_iteratorof(&it) == NULL);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_minmax1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 4, 0, &z);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == z);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_minmax2(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 1, 0, &z);
	addv(&b, &r, 2, 0, &i);
	addv(&b, &r, 3, 0, &j);
	addv(&b, &r, 4, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 16);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == z);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_mid0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_mid1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_iterate0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_iterate1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, k);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lt_eq(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	sv *v = sr_iteratorof(&it);
	t( v == NULL);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lt_minmax(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 7);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	t( sr_iteratorof(&it) == NULL);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lt_mid(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lt_iterate0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lt_iterate1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, k);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_dup_eq(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int j = 4;
	int i = 7;
	addv(&b, &r, 0, 0, &j);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0|SVDUP, &i);
	addv(&b, &r, 1, 0|SVDUP, &i);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 3 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 2 ); sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 1 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 0ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 0 );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_dup_mid(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 5, 0, &i);
	addv(&b, &r, 4, 0, &j);
	addv(&b, &r, 3, 0|SVDUP, &j);
	addv(&b, &r, 2, 0|SVDUP, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	sv *v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 2 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 3 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 4 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 10ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 4 );
	sv_vfree(&a, key);

	key = allocv(&r, 8);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 8ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i );
	t( sv_lsn(v) == 5 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_dup_mid_gt(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 90, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 70, 0|SVDUP, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 16);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	sv *v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 30ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 38ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 40ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 50ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 90ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_dup_mid_lt(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 3;
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	addv(&b, &r, 90, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 70, 0|SVDUP, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 30ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 38ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 40ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 50ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 90ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_dup_iterate0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 90, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 70, 0|SVDUP, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 100);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 100ULL);

	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 90);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_lte_dup_iterate1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 42, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	addv(&b, &r, 41, 0|SVDUP, &j);
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 100);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 30ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, sv_vpointer(key), key->size, 42ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 41);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_LTE, NULL, 0, 42ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 41);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_eq0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_eq1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_eq2(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	sv *v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_minmax0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_minmax1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 4, 0, &z);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_minmax2(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 4, 0, &z);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 2);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == z);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_mid0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 2);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_mid1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 1);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_iterate0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_iterate1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gt_eq(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( v != NULL);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gt_minmax(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 7);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 15);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gt_mid(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gt_iterate0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gt_iterate1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GT, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_dup_eq(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int j = 4;
	int i = 7;
	addv(&b, &r, 4, 0, &j);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0|SVDUP, &i);
	addv(&b, &r, 1, 0|SVDUP, &i);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 4 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 3 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 2 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 1 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 0ULL);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_dup_mid(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 5, 0, &i);
	addv(&b, &r, 4, 0, &j);
	addv(&b, &r, 3, 0|SVDUP, &j);
	addv(&b, &r, 2, 0|SVDUP, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 4 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 1ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k );
	t( sv_lsn(v) == 1 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k );
	t( sv_lsn(v) == 1 );
	sv_vfree(&a, key);

	key = allocv(&r, 8);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 8ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 4 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 3ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 3 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 2ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 2 );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 6ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_dup_mid_gt(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 90, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 70, 0|SVDUP, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 4ULL);
	t( sr_iteratorhas(&it) == 0 );
	sv *v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 30ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 38ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 40ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 50ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 90ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_dup_mid_lt(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int k = 7;
	int i = 8;
	int j = 9;
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	addv(&b, &r, 90, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 70, 0|SVDUP, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 30ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 38ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 40ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 50ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 90ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_dup_iterate0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 90, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 70, 0|SVDUP, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 1);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 100ULL);

	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 90);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sr_iteratornext(&it);

	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_gte_dup_iterate1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 42, 0, &i);
	addv(&b, &r, 80, 0, &j);
	addv(&b, &r, 60, 0|SVDUP, &j);
	addv(&b, &r, 41, 0|SVDUP, &j);
	addv(&b, &r, 50, 0, &k);
	addv(&b, &r, 40, 0|SVDUP, &k);
	addv(&b, &r, 30, 0|SVDUP, &k);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 1);
	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 30ULL);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, sv_vpointer(key), key->size, 42ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 41);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );

	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, 60ULL);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 60);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	v = sr_iteratorof(&it);
	t( v == NULL );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

static void
sdpageiter_update0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);
	int i = 0;
	for (; i < 10; i++)
		addv(&b, &r, i, 0, &i);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	svv *key = allocv(&r, 5);
	i = 5;
	t( sr_iteropen(sd_pageiter, &it, &page, SR_UPDATE, sv_vpointer(key), key->size, (uint64_t)i) == 0 );
	sr_iteratorclose(&it);

	sr_iterinit(sd_pageiter, &it, &r);
	i = 5;
	t( sr_iteropen(sd_pageiter, &it, &page, SR_UPDATE, sv_vpointer(key), key->size, (uint64_t)(i - 1)) == 1 );
	sr_iteratorclose(&it);

	sr_iterinit(sd_pageiter, &it, &r);
	i = 5;
	t( sr_iteropen(sd_pageiter, &it, &page, SR_UPDATE, sv_vpointer(key), key->size, (uint64_t)(i + 1)) == 0 );
	sr_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
	sr_keyfree(&cmp, &a);
}

stgroup *sdpageiter_group(void)
{
	stgroup *group = st_group("sdpageiter");
	st_groupadd(group, st_test("lte_empty", sdpageiter_lte_empty));
	st_groupadd(group, st_test("lte_eq0", sdpageiter_lte_eq0));
	st_groupadd(group, st_test("lte_eq1", sdpageiter_lte_eq1));
	st_groupadd(group, st_test("lte_eq2", sdpageiter_lte_eq2));
	st_groupadd(group, st_test("lte_minmax0",  sdpageiter_lte_minmax0));
	st_groupadd(group, st_test("lte_minmax1", sdpageiter_lte_minmax1));
	st_groupadd(group, st_test("lte_minmax2", sdpageiter_lte_minmax2));
	st_groupadd(group, st_test("lte_mid0", sdpageiter_lte_mid0));
	st_groupadd(group, st_test("lte_mid1", sdpageiter_lte_mid1));
	st_groupadd(group, st_test("lte_iterate0", sdpageiter_lte_iterate0));
	st_groupadd(group, st_test("lte_iterate1", sdpageiter_lte_iterate1));
	st_groupadd(group, st_test("lt_eq", sdpageiter_lt_eq));
	st_groupadd(group, st_test("lt_minmax", sdpageiter_lt_minmax));
	st_groupadd(group, st_test("lt_mid", sdpageiter_lt_mid));
	st_groupadd(group, st_test("lt_iterate0", sdpageiter_lt_iterate0));
	st_groupadd(group, st_test("lt_iterate1", sdpageiter_lt_iterate1));
	st_groupadd(group, st_test("lte_dup_eq", sdpageiter_lte_dup_eq));
	st_groupadd(group, st_test("lte_dup_mid", sdpageiter_lte_dup_mid));
	st_groupadd(group, st_test("lte_dup_mid_gt", sdpageiter_lte_dup_mid_gt));
	st_groupadd(group, st_test("lte_dup_mid_lt", sdpageiter_lte_dup_mid_lt));
	st_groupadd(group, st_test("lte_dup_iterate0", sdpageiter_lte_dup_iterate0));
	st_groupadd(group, st_test("lte_dup_iterate1", sdpageiter_lte_dup_iterate1));
	st_groupadd(group, st_test("gte_eq0", sdpageiter_gte_eq0));
	st_groupadd(group, st_test("gte_eq1", sdpageiter_gte_eq1));
	st_groupadd(group, st_test("gte_eq2", sdpageiter_gte_eq2));
	st_groupadd(group, st_test("gte_minmax0", sdpageiter_gte_minmax0));
	st_groupadd(group, st_test("gte_minmax1", sdpageiter_gte_minmax1));
	st_groupadd(group, st_test("gte_minmax2", sdpageiter_gte_minmax2));
	st_groupadd(group, st_test("gte_mid0", sdpageiter_gte_mid0));
	st_groupadd(group, st_test("gte_mid1", sdpageiter_gte_mid1));
	st_groupadd(group, st_test("gte_iterate0", sdpageiter_gte_iterate0));
	st_groupadd(group, st_test("gte_iterate1", sdpageiter_gte_iterate1));
	st_groupadd(group, st_test("gt_eq", sdpageiter_gt_eq));
	st_groupadd(group, st_test("gt_minmax", sdpageiter_gt_minmax));
	st_groupadd(group, st_test("gt_mid", sdpageiter_gt_mid));
	st_groupadd(group, st_test("gt_iterate0", sdpageiter_gt_iterate0));
	st_groupadd(group, st_test("gt_iterate1", sdpageiter_gt_iterate1));
	st_groupadd(group, st_test("gte_dup_eq", sdpageiter_gte_dup_eq));
	st_groupadd(group, st_test("gte_dup_mid", sdpageiter_gte_dup_mid));
	st_groupadd(group, st_test("gte_dup_mid_gt", sdpageiter_gte_dup_mid_gt));
	st_groupadd(group, st_test("gte_dup_mid_lt", sdpageiter_gte_dup_mid_lt));
	st_groupadd(group, st_test("gte_dup_iterate0", sdpageiter_gte_dup_iterate0));
	st_groupadd(group, st_test("gte_dup_iterate1", sdpageiter_gte_dup_iterate1));
	st_groupadd(group, st_test("update0", sdpageiter_update0));
	return group;
}
