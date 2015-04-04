
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

static void
addv(sdbuild *b, sr *r, uint64_t lsn, uint8_t flags, int *key)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = key;
	l.keysize     = sizeof(int);
	l.value       = key;
	l.valuesize   = sizeof(int);
	sv lv;
	sv_init(&lv, &sv_localif, &l, NULL);
	sd_buildadd(b, r, &lv, flags & SVDUP);
}

static void
sdv_test(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	srcrcf crc = sr_crc32c_function();
	sr_init(&r, &error, &a, NULL, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);
	int i = 7;
	int j = 8;
	addv(&b, &r, 3, SVSET, &i);
	addv(&b, &r, 4, SVSET, &j);
	sd_buildend(&b, &r);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_buildwritepage(&b, &r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(sd_pageiter, &it, &r);
	sr_iteropen(sd_pageiter, &it, &page, SR_GTE, NULL, 0, UINT64_MAX);
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( v != NULL );
	t( v->i == &sd_vif );

	t( *(int*)sv_key(v) == i );
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) != 0 );

	v = sr_iteratorof(&it);
	t( v != NULL );
	t( v->i == &sd_vif );
	
	t( *(int*)sv_key(v) == j );
	t( sv_lsn(v) == 4 );
	t( sv_flags(v) == SVSET );
	sv_flagsadd(v, SVDUP);
	t( sv_flags(v) == (SVSET|SVDUP) );

	sd_buildfree(&b, &r);
	sr_buffree(&buf, &a);
}

stgroup *sdv_group(void)
{
	stgroup *group = st_group("sdv");
	st_groupadd(group, st_test("test", sdv_test));
	return group;
}
