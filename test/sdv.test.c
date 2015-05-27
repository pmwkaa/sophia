
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
	svv *v = sv_vbuild(r, &pv, 1, (char*)key, sizeof(uint32_t));
	v->lsn = lsn;
	v->flags = flags;
	sv vv;
	sv_init(&vv, &sv_vif, v, NULL);
	sd_buildadd(b, r, &vv, flags & SVDUP);
	sv_vfree(r->a, v);
}

static void
sdv_test(stc *cx ssunused)
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
	addv(&b, &r, 3, 0, &i);
	addv(&b, &r, 4, 0, &j);
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
	ss_iteropen(sd_pageiter, &it, &r, &xfbuf, &page, SS_GTE, NULL, 0, UINT64_MAX);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v != NULL );

	t( *(int*)sv_key(v, &r, 0) == i );
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );

	v = ss_iteratorof(&it);
	t( v != NULL );
	
	t( *(int*)sv_key(v, &r, 0) == j );
	t( sv_lsn(v) == 4 );
	t( sv_flags(v) == 0 );

	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

stgroup *sdv_group(void)
{
	stgroup *group = st_group("sdv");
	st_groupadd(group, st_test("test", sdv_test));
	return group;
}
