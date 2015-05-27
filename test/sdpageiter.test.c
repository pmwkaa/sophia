
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
#include <libsd.h>
#include <libst.h>
#include <sophia.h>

static void*
allocv(sr *r, int key)
{
	sfv pv;
	pv.key = (char*)&key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	return sv_vbuild(r, &pv, 1, NULL, 0);
}

static void
addv(sdbuild *b, sr *r, uint64_t lsn, uint8_t flags, int *key)
{
	sfv pv;
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
sdpageiter_lte_empty(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_eq0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_eq1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_eq2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_minmax0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	t( ss_iteratorof(&it) == NULL);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_minmax1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 4, 0, &z);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == z);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_minmax2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 1, 0, &z);
	addv(&b, &r, 2, 0, &i);
	addv(&b, &r, 3, 0, &j);
	addv(&b, &r, 4, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 16);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == z);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_mid0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_mid1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, k);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lt_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lt_minmax(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 7);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	t( ss_iteratorof(&it) == NULL);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lt_mid(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lt_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lt_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, k);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_dup_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int j = 4;
	int i = 7;
	addv(&b, &r, 0, 0, &j);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0|SVDUP, &i);
	addv(&b, &r, 1, 0|SVDUP, &i);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 3 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 2 ); ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 1 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 0ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 0 );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_dup_mid(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 5, 0, &i);
	addv(&b, &r, 4, 0, &j);
	addv(&b, &r, 3, 0|SVDUP, &j);
	addv(&b, &r, 2, 0|SVDUP, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 2 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 3 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 4 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 10ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 4 );
	sv_vfree(&a, key);

	key = allocv(&r, 8);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i );
	t( sv_lsn(v) == 5 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_dup_mid_gt(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 16);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 30ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 38ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 40ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 50ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 90ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_dup_mid_lt(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 30ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 38ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 40ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 50ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 90ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_dup_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 100);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 100ULL);

	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 90);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_lte_dup_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 100);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 30ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, sv_vpointer(key), key->size, 42ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 41);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_LTE, NULL, 0, 42ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 41);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_eq0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_eq1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_eq2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_minmax0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_minmax1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 4, 0, &z);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 16);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_minmax2(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 4, 0, &z);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 2);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == z);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_mid0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 2);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_mid1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	sv_vfree(&a, key);

	key = allocv(&r, 1);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gt_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v != NULL);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, k);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gt_minmax(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 7);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 15);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gt_mid(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 555);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gt_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gt_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, i);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GT, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_dup_eq(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int j = 4;
	int i = 7;
	addv(&b, &r, 4, 0, &j);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0|SVDUP, &i);
	addv(&b, &r, 1, 0|SVDUP, &i);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 4 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 3 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 2 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 1 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 0ULL);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_dup_mid(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, &r, 5, 0, &i);
	addv(&b, &r, 4, 0, &j);
	addv(&b, &r, 3, 0|SVDUP, &j);
	addv(&b, &r, 2, 0|SVDUP, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 8);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 4 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 1ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	sv_vfree(&a, key);

	key = allocv(&r, 10);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k );
	t( sv_lsn(v) == 1 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k );
	t( sv_lsn(v) == 1 );
	sv_vfree(&a, key);

	key = allocv(&r, 8);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 8ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 4 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 3ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 3 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 2ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 2 );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 6ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_dup_mid_gt(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 4ULL);
	t( ss_iteratorhas(&it) == 0 );
	sv *v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 30ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 38ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 40ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 50ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 90ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_dup_mid_lt(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 6);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 30ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 38ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 40ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 50ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	sv_vfree(&a, key);

	key = allocv(&r, j);
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 90ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_dup_iterate0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 1);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 100ULL);

	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 90);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 80);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	ss_iteratornext(&it);

	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_gte_dup_iterate1(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

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

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	svv *key = allocv(&r, 1);
	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 30ULL);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 30);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, sv_vpointer(key), key->size, 42ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 41);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 40);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );

	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, 60ULL);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == i);
	t( sv_lsn(v) == 42);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == j);
	t( sv_lsn(v) == 60);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == k);
	t( sv_lsn(v) == 50);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	v = ss_iteratorof(&it);
	t( v == NULL );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdpageiter_update0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);
	int i = 0;
	for (; i < 10; i++)
		addv(&b, &r, i, 0, &i);
	sd_buildend(&b, &r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );
	t( sd_commitpage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	svv *key = allocv(&r, 5);
	i = 5;
	t( ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_UPDATE, sv_vpointer(key), key->size, (uint64_t)i) == 0 );
	ss_iteratorclose(&it);

	ss_iterinit(sd_pageiter, &it);
	i = 5;
	t( ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_UPDATE, sv_vpointer(key), key->size, (uint64_t)(i - 1)) == 1 );
	ss_iteratorclose(&it);

	ss_iterinit(sd_pageiter, &it);
	i = 5;
	t( ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_UPDATE, sv_vpointer(key), key->size, (uint64_t)(i + 1)) == 0 );
	ss_iteratorclose(&it);
	sv_vfree(&a, key);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
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
