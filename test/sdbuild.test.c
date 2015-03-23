
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
	l.value       = NULL;
	l.valuesize   = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	sd_buildadd(b, &lv, flags & SVDUP);
}

static void
sdbuild_empty(stc *cx srunused)
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
	sd_buildend(&b, 1);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 0 );

	sd_buildfree(&b);
}

static void
sdbuild_page0(stc *cx srunused)
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
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	sd_buildend(&b, 1);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );
	sd_buildfree(&b);
}

static void
sdbuild_page1(stc *cx srunused)
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
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	sd_buildend(&b, 1);
	sdpageheader *h = sd_buildheader(&b);
	t( h->count == 3 );
	t( h->lsnmin == 1 );
	t( h->lsnmax == 3 );

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_buildwritepage(&b, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	sdv *min = sd_pagemin(&page);
	t( *(int*)sd_pagekey(&page, min) == i );
	sdv *max = sd_pagemax(&page);
	t( *(int*)sd_pagekey(&page, max) == k );
	sd_buildcommit(&b);

	t( sd_buildbegin(&b) == 0);
	j = 19; 
	k = 20;
	addv(&b, 4, SVSET, &j);
	addv(&b, 5, SVSET, &k);
	sd_buildend(&b, 1);
	h = sd_buildheader(&b);
	t( h->count == 2 );
	t( h->lsnmin == 4 );
	t( h->lsnmax == 5 );
	sr_bufreset(&buf);
	t( sd_buildwritepage(&b, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sd_pageinit(&page, h);
	min = sd_pagemin(&page);
	t( *(int*)sd_pagekey(&page, min) == j );
	max = sd_pagemax(&page);
	t( *(int*)sd_pagekey(&page, max) == k );
	sd_buildcommit(&b);

	sd_buildfree(&b);
	sr_buffree(&buf, &a);
}

stgroup *sdbuild_group(void)
{
	stgroup *group = st_group("sdbuild");
	st_groupadd(group, st_test("empty", sdbuild_empty));
	st_groupadd(group, st_test("page0", sdbuild_page0));
	st_groupadd(group, st_test("page1", sdbuild_page1));
	return group;
}
