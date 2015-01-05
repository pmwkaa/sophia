
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
sditer_gt0(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, &ij);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0, 0) == 0 );

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b) == 0);

	int key = 7;
	addv(&b, 3, SVSET, &key);
	key = 8;
	addv(&b, 4, SVSET, &key);
	key = 9;
	addv(&b, 5, SVSET, &key);
	sd_buildend(&b);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_buildwritepage(&b, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	int rc;
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->count,
	                 sd_pagekey(&page, sd_buildmin(&b)),
	                 sd_buildmin(&b)->keysize,
	                 sd_pagekey(&page, sd_buildmax(&b)),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	sdid id;
	memset(&id, 0, sizeof(id));
	sd_buildcommit(&b);

	t( sd_indexcommit(&index, &id) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_buildwrite(&b, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	sriter it;
	sr_iterinit(&it, &sd_iter, &r);
	sr_iteropen(&it, &i, map.p, 1);
	t( sr_iterhas(&it) == 1 );

	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == 7);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 8);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 9);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	sr_iterclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b);
	sr_buffree(&buf, &a);
}

static void
sditer_gt1(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, &ij);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0, 0) == 0 );

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b) == 0);

	int key = 7;
	addv(&b, 3, SVSET, &key);
	key = 8;
	addv(&b, 4, SVSET, &key);
	key = 9;
	addv(&b, 5, SVSET, &key);
	sd_buildend(&b);

	srbuf buf;
	sr_bufinit(&buf);
	t( sd_buildwritepage(&b, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	int rc;
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->count,
	                 sd_pagekey(&page, sd_buildmin(&b)),
	                 sd_buildmin(&b)->keysize,
	                 sd_pagekey(&page, sd_buildmax(&b)),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t(rc == 0);
	sd_buildcommit(&b);

	t( sd_buildbegin(&b) == 0);
	key = 10;
	addv(&b, 6, SVSET, &key);
	key = 11;
	addv(&b, 7, SVSET, &key);
	key = 13;
	addv(&b, 8, SVSET, &key);
	sd_buildend(&b);

	sr_bufreset(&buf);
	t( sd_buildwritepage(&b, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sd_pageinit(&page, h);

	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->count,
	                 sd_pagekey(&page, sd_buildmin(&b)),
	                 sd_buildmin(&b)->keysize,
	                 sd_pagekey(&page, sd_buildmax(&b)),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t(rc == 0);
	sd_buildcommit(&b);

	t( sd_buildbegin(&b) == 0);
	key = 15;
	addv(&b, 9, SVSET, &key);
	key = 18;
	addv(&b, 10, SVSET, &key);
	key = 20;
	addv(&b, 11, SVSET, &key);
	sd_buildend(&b);

	sr_bufreset(&buf);
	t( sd_buildwritepage(&b, &buf) == 0 );
	h = (sdpageheader*)buf.s;
	sd_pageinit(&page, h);

	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->count,
	                 sd_pagekey(&page, sd_buildmin(&b)),
	                 sd_buildmin(&b)->keysize,
	                 sd_pagekey(&page, sd_buildmax(&b)),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t(rc == 0);
	sd_buildcommit(&b);

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &id) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_buildwrite(&b, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	sriter it;
	sr_iterinit(&it, &sd_iter, &r);
	sr_iteropen(&it, &i, map.p, 1);
	t( sr_iterhas(&it) == 1 );

	/* page 0 */
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == 7);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 8);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 9);
	sr_iternext(&it);

	/* page 1 */
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 10);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 11);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 13);
	sr_iternext(&it);

	/* page 2 */
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 15);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 18);
	sr_iternext(&it);
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == 20);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	sr_iterclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b);
	sr_buffree(&buf, &a);
}

stgroup *sditer_group(void)
{
	stgroup *group = st_group("sditer");
	st_groupadd(group, st_test("gt0", sditer_gt0));
	st_groupadd(group, st_test("gt1", sditer_gt1));
	return group;
}
