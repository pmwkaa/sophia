
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
	srfmtv pv;
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
sditer_gt0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, SR_FS_RAW, &cmp, &ij, crc, NULL);

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

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	srbuf xfbuf;
	sr_bufinit(&xfbuf);
	t( sr_bufensure(&xfbuf, &a, 1024) == 0 );

	sriter it;
	sr_iterinit(sd_iter, &it, &r);
	sr_iteropen(sd_iter, &it, &i, map.p, 1, 0, NULL, &xfbuf);
	t( sr_iteratorhas(&it) == 1 );

	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	sr_iteratorclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
	sr_buffree(&xfbuf, &a);
}

static void
sditer_gt1(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, SR_FS_RAW, &cmp, &ij, crc, NULL);

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

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	srbuf xfbuf;
	sr_bufinit(&xfbuf);
	t( sr_bufensure(&xfbuf, &a, 1024) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	sriter it;
	sr_iterinit(sd_iter, &it, &r);
	sr_iteropen(sd_iter, &it, &i, map.p, 1, 0, NULL, &xfbuf);
	t( sr_iteratorhas(&it) == 1 );

	/* page 0 */
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	sr_iteratornext(&it);

	/* page 1 */
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 10);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 11);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 13);
	sr_iteratornext(&it);

	/* page 2 */
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 15);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 18);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 20);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	sr_iteratorclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	sr_schemefree(&cmp, &a);
	sr_buffree(&xfbuf, &a);
}

static void
sditer_gt0_compression_zstd(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, SR_FS_RAW, &cmp, &ij, crc, &sr_zstdfilter);

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

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	srbuf compression_buf;
	sr_bufinit(&compression_buf);
	srbuf xfbuf;
	sr_bufinit(&xfbuf);
	t( sr_bufensure(&xfbuf, &a, 1024) == 0 );

	sriter it;
	sr_iterinit(sd_iter, &it, &r);
	sr_iteropen(sd_iter, &it, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( sr_iteratorhas(&it) == 1 );

	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	sr_iteratorclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);

	sr_buffree(&compression_buf, &a);
	sr_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sditer_gt0_compression_lz4(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, SR_FS_RAW, &cmp, &ij, crc, &sr_lz4filter);

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

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);

	srbuf compression_buf;
	sr_bufinit(&compression_buf);
	srbuf xfbuf;
	sr_bufinit(&xfbuf);
	t( sr_bufensure(&xfbuf, &a, 1024) == 0 );

	sriter it;
	sr_iterinit(sd_iter, &it, &r);
	sr_iteropen(sd_iter, &it, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( sr_iteratorhas(&it) == 1 );

	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	sr_iteratorclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);

	sr_buffree(&compression_buf, &a);
	sr_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sditer_gt1_compression_zstd(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, SR_FS_RAW, &cmp, &ij, crc, &sr_zstdfilter);

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

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	srbuf compression_buf;
	sr_bufinit(&compression_buf);
	srbuf xfbuf;
	sr_bufinit(&xfbuf);
	t( sr_bufensure(&xfbuf, &a, 1024) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	sriter it;
	sr_iterinit(sd_iter, &it, &r);
	sr_iteropen(sd_iter, &it, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( sr_iteratorhas(&it) == 1 );

	/* page 0 */
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	sr_iteratornext(&it);

	/* page 1 */
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 10);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 11);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 13);
	sr_iteratornext(&it);

	/* page 2 */
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 15);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 18);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 20);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	sr_iteratorclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	sr_buffree(&compression_buf, &a);
	sr_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sditer_gt1_compression_lz4(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	srinjection ij;
	memset(&ij, 0, sizeof(ij));
	srerror error;
	sr_errorinit(&error);
	srseq seq;
	sr_seqinit(&seq);
	srcrcf crc = sr_crc32c_function();
	sr r;
	sr_init(&r, &error, &a, &seq, SR_FKV, SR_FS_RAW, &cmp, &ij, crc, &sr_lz4filter);

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

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sd_commit(&b, &r, &index, &f) == 0 );
	srmap map;
	t( sr_mapfile(&map, &f, 1) == 0 );

	srbuf compression_buf;
	sr_bufinit(&compression_buf);
	srbuf xfbuf;
	sr_bufinit(&xfbuf);
	t( sr_bufensure(&xfbuf, &a, 1024) == 0 );

	sdindex i;
	sd_indexinit(&i);
	i.h = (sdindexheader*)(map.p);
	sriter it;
	sr_iterinit(sd_iter, &it, &r);
	sr_iteropen(sd_iter, &it, &i, map.p, 1, 1, &compression_buf, &xfbuf);
	t( sr_iteratorhas(&it) == 1 );

	/* page 0 */
	t( sr_iteratorhas(&it) != 0 );
	sv *v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 7);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 8);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 9);
	sr_iteratornext(&it);

	/* page 1 */
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 10);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 11);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 13);
	sr_iteratornext(&it);

	/* page 2 */
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 15);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 18);
	sr_iteratornext(&it);
	v = sr_iteratorof(&it);
	t( *(int*)sv_key(v, &r, 0) == 20);
	sr_iteratornext(&it);
	t( sr_iteratorhas(&it) == 0 );
	sr_iteratorclose(&it);

	sr_fileclose(&f);
	t( sr_mapunmap(&map) == 0 );
	t( sr_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	sr_buffree(&compression_buf, &a);
	sr_buffree(&xfbuf, &a);
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
