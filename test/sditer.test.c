
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
sditer_gt0(stc *cx ssunused)
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
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b, &r);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &r, &id) == 0 );

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	ssmap map;
	t( ss_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	ssiter it;
	ss_iterinit(sd_iter, &it);
	ss_iteropen(sd_iter, &it, &r, &i, map.p, 1, 0, NULL, &xfbuf);
	t( ss_iteratorhas(&it) == 1 );

	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_mapunmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
	ss_buffree(&xfbuf, &a);
}

static void
sditer_gt1(stc *cx ssunused)
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
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, NULL);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b, &r);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);
	key = 10;
	addv(&b, &r, 6, 0, &key);
	key = 11;
	addv(&b, &r, 7, 0, &key);
	key = 13;
	addv(&b, &r, 8, 0, &key);
	sd_buildend(&b, &r);

	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	t( sd_buildbegin(&b, &r, 1, 0, 0) == 0);
	key = 15;
	addv(&b, &r, 9, 0, &key);
	key = 18;
	addv(&b, &r, 10, 0, &key);
	key = 20;
	addv(&b, &r, 11, 0, &key);
	sd_buildend(&b, &r);

	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id) == 0 );

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	ssmap map;
	t( ss_mapfile(&map, &f, 1) == 0 );

	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	ssiter it;
	ss_iterinit(sd_iter, &it);
	ss_iteropen(sd_iter, &it, &r, &i, map.p, 1, 0, NULL, &xfbuf);
	t( ss_iteratorhas(&it) == 1 );

	/* page 0 */
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	ss_iteratornext(&it);

	/* page 1 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 10);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 11);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 13);
	ss_iteratornext(&it);

	/* page 2 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 15);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 18);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 20);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_mapunmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
	ss_buffree(&xfbuf, &a);
}

static void
sditer_gt0_compression_zstd(stc *cx ssunused)
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
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_zstdfilter);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	t( sd_buildend(&b, &r) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &r, &id) == 0 );

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	ssmap map;
	t( ss_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	ssbuf compression_buf;
	ss_bufinit(&compression_buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	ssiter it;
	ss_iterinit(sd_iter, &it);
	ss_iteropen(sd_iter, &it, &r, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( ss_iteratorhas(&it) == 1 );

	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_mapunmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);

	ss_buffree(&compression_buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sditer_gt0_compression_lz4(stc *cx ssunused)
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
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_lz4filter);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	t( sd_buildend(&b, &r) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &r, &id) == 0 );

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	ssmap map;
	t( ss_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	ssbuf compression_buf;
	ss_bufinit(&compression_buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	ssiter it;
	ss_iterinit(sd_iter, &it);
	ss_iteropen(sd_iter, &it, &r, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( ss_iteratorhas(&it) == 1 );

	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_mapunmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);

	ss_buffree(&compression_buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sditer_gt1_compression_zstd(stc *cx ssunused)
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
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_zstdfilter);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b, &r);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	key = 10;
	addv(&b, &r, 6, 0, &key);
	key = 11;
	addv(&b, &r, 7, 0, &key);
	key = 13;
	addv(&b, &r, 8, 0, &key);
	sd_buildend(&b, &r);

	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	key = 15;
	addv(&b, &r, 9, 0, &key);
	key = 18;
	addv(&b, &r, 10, 0, &key);
	key = 20;
	addv(&b, &r, 11, 0, &key);
	sd_buildend(&b, &r);

	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id) == 0 );

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	ssmap map;
	t( ss_mapfile(&map, &f, 1) == 0 );

	ssbuf compression_buf;
	ss_bufinit(&compression_buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	ssiter it;
	ss_iterinit(sd_iter, &it);
	ss_iteropen(sd_iter, &it, &r, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( ss_iteratorhas(&it) == 1 );

	/* page 0 */
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	ss_iteratornext(&it);

	/* page 1 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 10);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 11);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 13);
	ss_iteratornext(&it);

	/* page 2 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 15);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 18);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 20);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_mapunmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	ss_buffree(&compression_buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sditer_gt1_compression_lz4(stc *cx ssunused)
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
	sr_init(&r, &error, &a, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_lz4filter);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b, &r);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r, 0) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	key = 10;
	addv(&b, &r, 6, 0, &key);
	key = 11;
	addv(&b, &r, 7, 0, &key);
	key = 13;
	addv(&b, &r, 8, 0, &key);
	sd_buildend(&b, &r);

	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	t( sd_buildbegin(&b, &r, 1, 1, 0) == 0);
	key = 15;
	addv(&b, &r, 9, 0, &key);
	key = 18;
	addv(&b, &r, 10, 0, &key);
	key = 20;
	addv(&b, &r, 11, 0, &key);
	sd_buildend(&b, &r);

	rc = sd_indexadd(&index, &r, &b);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id) == 0 );

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	ssmap map;
	t( ss_mapfile(&map, &f, 1) == 0 );

	ssbuf compression_buf;
	ss_bufinit(&compression_buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	ssiter it;
	ss_iterinit(sd_iter, &it);
	ss_iteropen(sd_iter, &it, &r, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( ss_iteratorhas(&it) == 1 );

	/* page 0 */
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	ss_iteratornext(&it);

	/* page 1 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 10);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 11);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 13);
	ss_iteratornext(&it);

	/* page 2 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 15);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 18);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 20);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_mapunmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	ss_buffree(&compression_buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

stgroup *sditer_group(void)
{
	stgroup *group = st_group("sditer");
	st_groupadd(group, st_test("gt0", sditer_gt0));
	st_groupadd(group, st_test("gt1", sditer_gt1));
	st_groupadd(group, st_test("gt0_compression_zstd", sditer_gt0_compression_zstd));
	st_groupadd(group, st_test("gt0_compression_lz4", sditer_gt0_compression_lz4));
	st_groupadd(group, st_test("gt1_compression_zstd", sditer_gt1_compression_zstd));
	st_groupadd(group, st_test("gt1_compression_lz4", sditer_gt1_compression_lz4));
	return group;
}
