
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
	sfv pv;
	pv.key = (char*)key;
	pv.r.size = sizeof(uint32_t);
	pv.r.offset = 0;
	svv *v = sv_vbuild(r, &pv, 1, NULL, 0);
	v->lsn = lsn;
	v->flags = flags;
	sv vv;
	sv_init(&vv, &sv_vif, v, NULL);
	sd_buildadd(b, &vv, flags & SVDUP);
	sv_vfree(r->a, v);
}

static void
sd_read_gt0(void)
{
	sdbuild b;
	sd_buildinit(&b, &st_r.r);
	t( sd_buildbegin(&b, 1, 0, 0) == 0);

	int key = 7;
	addv(&b, &st_r.r, 3, 0, &key);
	key = 8;
	addv(&b, &st_r.r, 4, 0, &key);
	key = 9;
	addv(&b, &st_r.r, 5, 0, &key);
	sd_buildend(&b);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &st_r.r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &st_r.r, &b, sizeof(sdseal));
	t( rc == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	ssfile f;
	ss_fileinit(&f, &st_r.a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&st_r.r, &f, NULL) == 0 );
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );
	t( sd_indexcommit(&index, &st_r.r, &id, f.size) == 0 );
	t( sd_writeindex(&st_r.r, &f, NULL, &index) == 0 );
	t( sd_seal(&st_r.r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_mmap(&map, f.fd, f.size, 1) == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &st_r.a, 1024) == 0 );

	ssiter index_iter;
	ssiter page_iter;

	sdreadarg arg = {
		.index           = &index,
		.buf             = &buf,
		.buf_xf          = &xfbuf,
		.buf_read        = NULL,
		.index_iter      = &index_iter,
		.page_iter       = &page_iter,
		.vlsn            = 0,
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.has             = 0,
		.use_compression = 0,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.r               = &st_r.r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL, 0);
	t( ss_iteratorhas(&it) == 1 );

	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 9);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_munmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &st_r.r);
	sd_buildfree(&b);
	ss_buffree(&xfbuf, &st_r.a);
	ss_buffree(&buf, &st_r.a);
}

static void
sd_read_gt1(void)
{
	ssfile f;
	ss_fileinit(&f, &st_r.a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&st_r.r, &f, NULL) == 0 );

	sdbuild b;
	sd_buildinit(&b, &st_r.r);
	t( sd_buildbegin(&b, 1, 0, 0) == 0);

	int key = 7;
	addv(&b, &st_r.r, 3, 0, &key);
	key = 8;
	addv(&b, &st_r.r, 4, 0, &key);
	key = 9;
	addv(&b, &st_r.r, 5, 0, &key);
	sd_buildend(&b);
	uint64_t poff = f.size;
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &st_r.r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &st_r.r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	t( sd_buildbegin(&b, 1, 0, 0) == 0);
	key = 10;
	addv(&b, &st_r.r, 6, 0, &key);
	key = 11;
	addv(&b, &st_r.r, 7, 0, &key);
	key = 13;
	addv(&b, &st_r.r, 8, 0, &key);
	sd_buildend(&b);
	poff = f.size;
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &st_r.r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	t( sd_buildbegin(&b, 1, 0, 0) == 0);
	key = 15;
	addv(&b, &st_r.r, 9, 0, &key);
	key = 18;
	addv(&b, &st_r.r, 10, 0, &key);
	key = 20;
	addv(&b, &st_r.r, 11, 0, &key);
	sd_buildend(&b);
	poff = f.size;
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &st_r.r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &st_r.r, &id, f.size) == 0 );
	t( sd_writeindex(&st_r.r, &f, NULL, &index) == 0 );
	t( sd_seal(&st_r.r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_mmap(&map, f.fd, f.size, 1) == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &st_r.a, 1024) == 0 );

	ssiter index_iter;
	ssiter page_iter;

	sdreadarg arg = {
		.index           = &index,
		.buf             = &buf,
		.buf_xf          = &xfbuf,
		.buf_read        = NULL,
		.index_iter      = &index_iter,
		.page_iter       = &page_iter,
		.vlsn            = 0,
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.has             = 0,
		.use_compression = 0,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.r               = &st_r.r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL, 0);
	t( ss_iteratorhas(&it) == 1 );

	/* page 0 */
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 9);
	ss_iteratornext(&it);

	/* page 1 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 10);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 11);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 13);
	ss_iteratornext(&it);

	/* page 2 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 15);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 18);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_key(v, &st_r.r, 0) == 20);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_munmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &st_r.r);
	sd_buildfree(&b);
	ss_buffree(&xfbuf, &st_r.a);
	ss_buffree(&buf, &st_r.a);
}

static void
sd_read_gt0_compression_zstd(void)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_zstdfilter);

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b, 1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	t( sd_buildend(&b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, sizeof(sdseal));
	t( rc == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	ssfile f;
	ss_fileinit(&f, &st_r.a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );
	t( sd_writepage(&r, &f, NULL, &b) == 0 );
	t( sd_indexcommit(&index, &r, &id, f.size) == 0 );
	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	t( sd_buildcommit(&b) == 0 );

	ssmmap map;
	t( ss_mmap(&map, f.fd, f.size, 1) == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	ssiter index_iter;
	ssiter page_iter;

	sdreadarg arg = {
		.index           = &index,
		.buf             = &buf,
		.buf_xf          = &xfbuf,
		.buf_read        = NULL,
		.index_iter      = &index_iter,
		.page_iter       = &page_iter,
		.vlsn            = 0,
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.has             = 0,
		.use_compression = 1,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL, 0);
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
	t( ss_munmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b);

	ss_buffree(&xfbuf, &a);
	ss_buffree(&buf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sd_read_gt0_compression_lz4(void)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_lz4filter);

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b,1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	t( sd_buildend(&b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, sizeof(sdseal));
	t( rc == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &r, &id, 0) == 0 );

	ssfile f;
	ss_fileinit(&f, &st_r.a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );
	t( sd_writepage(&r, &f, NULL, &b) == 0 );
	t( sd_indexcommit(&index, &r, &id, f.size) == 0 );
	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_mmap(&map, f.fd, f.fd, 1) == 0 );

	t( sd_buildcommit(&b) == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	ssiter index_iter;
	ssiter page_iter;

	sdreadarg arg = {
		.index           = &index,
		.buf             = &buf,
		.buf_xf          = &xfbuf,
		.buf_read        = NULL,
		.index_iter      = &index_iter,
		.page_iter       = &page_iter,
		.vlsn            = 0,
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.has             = 0,
		.use_compression = 1,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL, 0);
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
	t( ss_munmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b);

	ss_buffree(&xfbuf, &a);
	ss_buffree(&buf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sd_read_gt1_compression_zstd(void)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_zstdfilter);

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b, 1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b);
	uint64_t poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );
	sd_buildreset(&b);

	t( sd_buildbegin(&b, 1, 1, 0) == 0);
	key = 10;
	addv(&b, &r, 6, 0, &key);
	key = 11;
	addv(&b, &r, 7, 0, &key);
	key = 13;
	addv(&b, &r, 8, 0, &key);
	sd_buildend(&b);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );
	sd_buildreset(&b);

	t( sd_buildbegin(&b, 1, 1, 0) == 0);
	key = 15;
	addv(&b, &r, 9, 0, &key);
	key = 18;
	addv(&b, &r, 10, 0, &key);
	key = 20;
	addv(&b, &r, 11, 0, &key);
	sd_buildend(&b);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id, f.size) == 0 );

	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_mmap(&map, f.fd, f.size, 1) == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	ssiter index_iter;
	ssiter page_iter;

	sdreadarg arg = {
		.index           = &index,
		.buf             = &buf,
		.buf_xf          = &xfbuf,
		.buf_read        = NULL,
		.index_iter      = &index_iter,
		.page_iter       = &page_iter,
		.vlsn            = 0,
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.has             = 0,
		.use_compression = 1,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL, 0);
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
	t( ss_munmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

static void
sd_read_gt1_compression_lz4(void)
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
	sr_init(&r, &error, &a, NULL, &seq, SF_KV, SF_SRAW, NULL, &cmp, &ij, crc, &ss_lz4filter);

	ssfile f;
	ss_fileinit(&f, &a);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );

	sdbuild b;
	sd_buildinit(&b, &r);
	t( sd_buildbegin(&b, 1, 1, 0) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b);
	uint64_t poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );
	sd_buildreset(&b);

	t( sd_buildbegin(&b, 1, 1, 0) == 0);
	key = 10;
	addv(&b, &r, 6, 0, &key);
	key = 11;
	addv(&b, &r, 7, 0, &key);
	key = 13;
	addv(&b, &r, 8, 0, &key);
	sd_buildend(&b);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );
	sd_buildreset(&b);

	t( sd_buildbegin(&b, 1, 1, 0) == 0);
	key = 15;
	addv(&b, &r, 9, 0, &key);
	key = 18;
	addv(&b, &r, 10, 0, &key);
	key = 20;
	addv(&b, &r, 11, 0, &key);
	sd_buildend(&b);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id, f.size) == 0 );

	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_mmap(&map, f.fd, f.size, 1) == 0 );

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &a, 1024) == 0 );

	ssiter index_iter;
	ssiter page_iter;

	sdreadarg arg = {
		.index           = &index,
		.buf             = &buf,
		.buf_xf          = &xfbuf,
		.buf_read        = NULL,
		.index_iter      = &index_iter,
		.page_iter       = &page_iter,
		.vlsn            = 0,
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.has             = 0,
		.use_compression = 1,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL, 0);
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
	t( ss_munmap(&map) == 0 );
	t( ss_fileunlink("./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sr_schemefree(&cmp, &a);
}

stgroup *sd_read_group(void)
{
	stgroup *group = st_group("sdread");
	st_groupadd(group, st_test("gt0", sd_read_gt0));
	st_groupadd(group, st_test("gt1", sd_read_gt1));
	st_groupadd(group, st_test("gt0_compression_zstd", sd_read_gt0_compression_zstd));
	st_groupadd(group, st_test("gt0_compression_lz4", sd_read_gt0_compression_lz4));
	st_groupadd(group, st_test("gt1_compression_zstd", sd_read_gt1_compression_zstd));
	st_groupadd(group, st_test("gt1_compression_lz4", sd_read_gt1_compression_lz4));
	return group;
}
