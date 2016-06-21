
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
	sfv pv[8];
	memset(pv, 0, sizeof(pv));
	pv[0].pointer = (char*)key;
	pv[0].size = sizeof(uint32_t);
	pv[1].pointer = NULL;
	pv[1].size = 0;
	svv *v = sv_vbuild(r, pv);
	sf_lsnset(r->scheme, sv_vpointer(v), lsn);
	sf_flagsset(r->scheme, sv_vpointer(v), flags);
	sv vv;
	sv_init(&vv, &sv_vif, v, NULL);
	sd_buildadd(b, r, &vv, flags & SVDUP);
	sv_vunref(r, v);
}

static void
sd_build_empty(void)
{
	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &st_r.r, 1, 0, 0, NULL) == 0);
	sd_buildend(&b, &st_r.r);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 0 );
	sd_buildfree(&b, &st_r.r);
}

static void
sd_build_page0(void)
{
	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &st_r.r, 1, 0, 0, NULL) == 0);
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, &st_r.r, 3, 0, &i);
	addv(&b, &st_r.r, 2, 0, &j);
	addv(&b, &st_r.r, 1, 0, &k);
	sd_buildend(&b, &st_r.r);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildfree(&b, &st_r.r);
}

static void
sd_build_compression_zstd(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssvfs vfs;
	ss_vfsinit(&vfs, &ss_stdvfs);
	sfscheme cmp;
	sf_schemeinit(&cmp);
	sffield *field = sf_fieldnew(&a, "key");
	t( sf_fieldoptions(field, &a, "u32,key(0)") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	field = sf_fieldnew(&a, "value");
	t( sf_fieldoptions(field, &a, "string") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	t( sf_schemevalidate(&cmp, &a) == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srstat stat;
	memset(&stat, 0, sizeof(stat));
	srlog log;
	sr_loginit(&log);
	srerror error;
	sr_errorinit(&error, &log);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, NULL, &log, &error, &a, &vfs, NULL, &seq, SF_RAW,
	        NULL, &cmp, &ij, &stat, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);
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

	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);
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
	sf_schemefree(&cmp, &a);
}

static void
sd_build_compression_lz4(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssvfs vfs;
	ss_vfsinit(&vfs, &ss_stdvfs);
	sfscheme cmp;
	sf_schemeinit(&cmp);
	sffield *field = sf_fieldnew(&a, "key");
	t( sf_fieldoptions(field, &a, "u32,key(0)") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	field = sf_fieldnew(&a, "value");
	t( sf_fieldoptions(field, &a, "string") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	t( sf_schemevalidate(&cmp, &a) == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srstat stat;
	memset(&stat, 0, sizeof(stat));
	srlog log;
	sr_loginit(&log);
	srerror error;
	sr_errorinit(&error, &log);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, NULL, &log, &error, &a, &vfs, NULL, &seq,
	        SF_RAW, NULL, &cmp, &ij, &stat, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);
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

	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);
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
	sf_schemefree(&cmp, &a);
}

stgroup *sd_build_group(void)
{
	stgroup *group = st_group("sdbuild");
	st_groupadd(group, st_test("empty", sd_build_empty));
	st_groupadd(group, st_test("page0", sd_build_page0));
	st_groupadd(group, st_test("compression_zstd", sd_build_compression_zstd));
	st_groupadd(group, st_test("compression_lz4", sd_build_compression_lz4));
	return group;
}
