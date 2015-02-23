
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
addv(sdbuild *b, uint64_t lsn, uint8_t flags, int *key)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = key;
	l.keysize     = sizeof(int);
	l.value       = key;
	l.valuesize   = sizeof(int);
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	sd_buildadd(b, &lv, flags & SVDUP);
}

static void
sdv_test(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	srcrcf crc = sr_crc32c_function();
	sr_init(&r, &error, &a, NULL, &cmp, &ij, crc);

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b) == 0);
	int i = 7;
	int j = 8;
	addv(&b, 3, SVSET, &i);
	addv(&b, 4, SVSET, &j);
	sd_buildend(&b);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_buildwritepage(&b, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sriter it;
	sr_iterinit(&it, &sd_pageiter, &r);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, UINT64_MAX);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v != NULL );
	t( v->i == &sd_vif );

	t( *(int*)svkey(v) == i );
	t( svvalueoffset(v) == 4 );
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );

	v = sr_iterof(&it);
	t( v != NULL );
	t( v->i == &sd_vif );
	
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 4 );
	t( svflags(v) == SVSET );
	svflagsadd(v, SVDUP);
	t( svflags(v) == (SVSET|SVDUP) );

	t( svvalueoffset(v) == 12 );

	sd_buildfree(&b);
	sr_buffree(&buf, &a);
}

stgroup *sdv_group(void)
{
	stgroup *group = st_group("sdv");
	st_groupadd(group, st_test("test", sdv_test));
	return group;
}
