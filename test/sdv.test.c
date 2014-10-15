
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
	l.valueoffset = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	sd_buildadd(b, &lv);
}

static void
sdv_test(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b, sizeof(int)) == 0);
	int i = 7;
	int j = 8;
	addv(&b, 3, SVSET, &i);
	addv(&b, 4, SVSET, &j);
	sd_buildend(&b);

	sdpageheader *h = sd_buildheader(&b);
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
	t( svvalueoffset(v) == 0 );
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );

	v = sr_iterof(&it);
	t( v != NULL );
	t( v->i == &sd_vif );
	
	t( *(int*)svkey(v) == j );
	t( svvalueoffset(v) == sizeof(int) );

	sd_buildfree(&b);
}

stgroup *sdv_group(void)
{
	stgroup *group = st_group("sdv");
	st_groupadd(group, st_test("test", sdv_test));
	return group;
}
