
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
	l.value       = NULL;
	l.valuesize   = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	sd_buildadd(b, r, &lv, flags & SVDUP);
}

static void
sditer_gt0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	sr r;
	srcrcf crc = sr_crc32c_function();
	sr_init(&r, &error, &a, NULL, &cmp, &ij, crc, NULL);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0, 0) == 0 );

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, SVSET, &key);
	key = 8;
	addv(&b, &r, 4, SVSET, &key);
	key = 9;
	addv(&b, &r, 5, SVSET, &key);
	sd_buildend(&b, &r);

	sdpageheader *h = sd_buildheader(&b);
	int rc;
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &r, &id) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_buildwrite(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	sriter it;
	sr_iterinit(&it, &sd_iter, &r);
	sr_iteropen(&it, &i, map.p, 1, 0, NULL);
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
	sd_buildfree(&b, &r);
}

static void
sditer_gt1(stc *cx srunused)
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

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0, 0) == 0 );

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, SVSET, &key);
	key = 8;
	addv(&b, &r, 4, SVSET, &key);
	key = 9;
	addv(&b, &r, 5, SVSET, &key);
	sd_buildend(&b, &r);

	sdpageheader *h = sd_buildheader(&b);
	int rc;
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	t( sd_buildbegin(&b, &r, 1, 0) == 0);
	key = 10;
	addv(&b, &r, 6, SVSET, &key);
	key = 11;
	addv(&b, &r, 7, SVSET, &key);
	key = 13;
	addv(&b, &r, 8, SVSET, &key);
	sd_buildend(&b, &r);

	h = sd_buildheader(&b);
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	t( sd_buildbegin(&b, &r, 1, 0) == 0);
	key = 15;
	addv(&b, &r, 9, SVSET, &key);
	key = 18;
	addv(&b, &r, 10, SVSET, &key);
	key = 20;
	addv(&b, &r, 11, SVSET, &key);
	sd_buildend(&b, &r);

	h = sd_buildheader(&b);
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_buildwrite(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	sriter it;
	sr_iterinit(&it, &sd_iter, &r);
	sr_iteropen(&it, &i, map.p, 1, 0, NULL);
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
	sd_buildfree(&b, &r);
}

static void
sditer_gt0_compression(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srcomparator cmp = { sr_cmpu32, NULL };
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	sr r;
	srcrcf crc = sr_crc32c_function();
	sr_init(&r, &error, &a, NULL, &cmp, &ij, crc, &sr_zstdfilter);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0, 0) == 0 );

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1) == 0);

	int key = 7;
	addv(&b, &r, 3, SVSET, &key);
	key = 8;
	addv(&b, &r, 4, SVSET, &key);
	key = 9;
	addv(&b, &r, 5, SVSET, &key);
	t( sd_buildend(&b, &r) == 0 );

	sdpageheader *h = sd_buildheader(&b);
	int rc;
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));


	t( sd_indexcommit(&index, &r, &id) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_buildwrite(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	srbuf compression_buf;
	sr_bufinit(&compression_buf);

	sriter it;
	sr_iterinit(&it, &sd_iter, &r);
	sr_iteropen(&it, &i, map.p, 1, 1, &compression_buf);
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
	sd_buildfree(&b, &r);

	sr_buffree(&compression_buf, &a);
}

static void
sditer_gt1_compression(stc *cx srunused)
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
	sr_init(&r, &error, &a, NULL, &cmp, &ij, crc, &sr_zstdfilter);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0, 0) == 0 );

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1) == 0);

	int key = 7;
	addv(&b, &r, 3, SVSET, &key);
	key = 8;
	addv(&b, &r, 4, SVSET, &key);
	key = 9;
	addv(&b, &r, 5, SVSET, &key);
	sd_buildend(&b, &r);

	sdpageheader *h = sd_buildheader(&b);
	int rc;
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	t( sd_buildbegin(&b, &r, 1, 1) == 0);
	key = 10;
	addv(&b, &r, 6, SVSET, &key);
	key = 11;
	addv(&b, &r, 7, SVSET, &key);
	key = 13;
	addv(&b, &r, 8, SVSET, &key);
	sd_buildend(&b, &r);

	h = sd_buildheader(&b);
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	t( sd_buildbegin(&b, &r, 1, 1) == 0);
	key = 15;
	addv(&b, &r, 9, SVSET, &key);
	key = 18;
	addv(&b, &r, 10, SVSET, &key);
	key = 20;
	addv(&b, &r, 11, SVSET, &key);
	sd_buildend(&b, &r);

	h = sd_buildheader(&b);
	rc = sd_indexadd(&index, &r,
	                 sd_buildoffset(&b),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 sd_buildminkey(&b),
	                 sd_buildmin(&b)->keysize,
	                 sd_buildmaxkey(&b),
	                 sd_buildmax(&b)->keysize,
	                 h->countdup,
	                 h->lsnmindup,
	                 h->lsnmin,
	                 h->lsnmax);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_buildwrite(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	srbuf compression_buf;
	sr_bufinit(&compression_buf);

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	sriter it;
	sr_iterinit(&it, &sd_iter, &r);
	sr_iteropen(&it, &i, map.p, 1, 1, &compression_buf);
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
	sd_buildfree(&b, &r);
	sr_buffree(&compression_buf, &a);
}

stgroup *sditer_group(void)
{
	stgroup *group = st_group("sditer");
	st_groupadd(group, st_test("gt0", sditer_gt0));
	st_groupadd(group, st_test("gt1", sditer_gt1));
	st_groupadd(group, st_test("gt0_compression", sditer_gt0_compression));
	st_groupadd(group, st_test("gt1_compression", sditer_gt1_compression));
	return group;
}
