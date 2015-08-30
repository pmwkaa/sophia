
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
#include <libsd.h>
#include <libst.h>

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
	sd_buildadd(b, &vv, flags & SVDUP);
	sv_vfree(r->a, v);
}

static void
sd_build_empty(void)
{
	sdbuild b;
	sd_buildinit(&b, &st_r.r);
	t( sd_buildbegin(&b, 1, 0, 0) == 0);
	sd_buildend(&b);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 0 );
	sd_buildfree(&b);
}

static void
sd_build_page0(void)
{
	sdbuild b;
	sd_buildinit(&b, &st_r.r);
	t( sd_buildbegin(&b, 1, 0, 0) == 0);
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &st_r.r, 3, 0, &i);
	addv(&b, &st_r.r, 2, 0, &j);
	addv(&b, &st_r.r, 1, 0, &k);
	sd_buildend(&b);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildfree(&b);
}

static void
sd_build_page1(void)
{
	sdbuild b;
	sd_buildinit(&b, &st_r.r);
	t( sd_buildbegin(&b, 1, 0, 0) == 0);
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &st_r.r, 3, 0, &i);
	addv(&b, &st_r.r, 2, 0, &j);
	addv(&b, &st_r.r, 1, 0, &k);
	sd_buildend(&b);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );

	ssbuf buf;
	ss_bufinit(&buf);
	t( sd_commitpage(&b, &st_r.r, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	uint64_t size, lsn;
	sdv *min = sd_pagemin(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, min, &size, &lsn), 0) == i );
	sdv *max = sd_pagemax(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, max, &size, &lsn), 0) == k );
	sd_buildcommit(&b);

	t( sd_buildbegin(&b, 1, 0, 0) == 0);
	j = 19; 
	k = 20;
	addv(&b, &st_r.r, 4, 0, &j);
	addv(&b, &st_r.r, 5, 0, &k);
	sd_buildend(&b);
	h = sd_buildheader(&b);
	t( h->count == 2 );
	t( h->lsnmin == 4 );
	t( h->lsnmax == 5 );
	ss_bufreset(&buf);
	t( sd_commitpage(&b, &st_r.r, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sd_pageinit(&page, h);
	min = sd_pagemin(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, min, &size, &lsn), 0) == j );
	max = sd_pagemax(&page);
	t( *(int*)sf_key( sd_pagemetaof(&page, max, &size, &lsn), 0) == k );
	sd_buildcommit(&b);

	sd_buildfree(&b);
	ss_buffree(&buf, &st_r.a);
}

static void
sd_build_compression_zstd(void)
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
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b,1, 1, 0) == 0);
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b);

	t( sd_buildbegin(&b, 1, 1, 0) == 0);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b);
	h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b);

	sd_buildfree(&b);
	sr_schemefree(&cmp, &a);
}

static void
sd_build_compression_lz4(void)
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
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b, 1, 1, 0) == 0);
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b);

	t( sd_buildbegin(&b, 1, 1, 0) == 0);
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 2, 0, &j);
	addv(&b, &r, 1, 0, &k);
	sd_buildend(&b);
	h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildcommit(&b);

	sd_buildfree(&b);
	sr_schemefree(&cmp, &a);
}

stgroup *sd_build_group(void)
{
	stgroup *group = st_group("sdbuild");
	st_groupadd(group, st_test("empty", sd_build_empty));
	st_groupadd(group, st_test("page0", sd_build_page0));
	st_groupadd(group, st_test("page1", sd_build_page1));
	st_groupadd(group, st_test("compression_zstd", sd_build_compression_zstd));
	st_groupadd(group, st_test("compression_lz4", sd_build_compression_lz4));
	return group;
}
