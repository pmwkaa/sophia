
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
sdbuild_empty(stc *cx ssunused)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);
	sd_buildend(&b, &r);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 0 );

	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
}

static void
sdbuild_page0(stc *cx ssunused)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, NULL);

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
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
}

static void
sdbuild_page1(stc *cx ssunused)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, NULL);

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
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );

	ssbuf buf;
	ss_bufinit(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	uint64_t size, lsn;
	sdv *min = sd_pagemin(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, min, &size, &lsn), 0) == i );
	sdv *max = sd_pagemax(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, max, &size, &lsn), 0) == k );
	sd_buildcommit(&b, &r);

	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);
	j = 19; 
	k = 20;
	addv(&b, &r, 4, 0, &j);
	addv(&b, &r, 5, 0, &k);
	sd_buildend(&b, &r);
	h = sd_buildheader(&b);
	t( h->count == 2 );
	t( h->lsnmin == 4 );
	t( h->lsnmax == 5 );
	ss_bufreset(&buf);
	t( sd_commitpage(&b, &r, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sd_pageinit(&page, h);
	min = sd_pagemin(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, min, &size, &lsn), 0) == j );
	max = sd_pagemax(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, max, &size, &lsn), 0) == k );
	sd_buildcommit(&b, &r);

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sdbuild_compression_zstd(stc *cx ssunused)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_zstdfilter);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b, &r);

	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);
	h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b, &r);

	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
}

static void
sdbuild_compression_lz4(stc *cx ssunused)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_lz4filter);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b, &r);

	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b, &r);
	h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b, &r);

	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
}

stgroup *sdbuild_group(void)
{
	stgroup *group = st_group("sdbuild");
	st_groupadd(group, st_test("empty", sdbuild_empty));
	st_groupadd(group, st_test("page0", sdbuild_page0));
	st_groupadd(group, st_test("page1", sdbuild_page1));
	st_groupadd(group, st_test("compression_zstd", sdbuild_compression_zstd));
	st_groupadd(group, st_test("compression_lz4", sdbuild_compression_lz4));
	return group;
}
