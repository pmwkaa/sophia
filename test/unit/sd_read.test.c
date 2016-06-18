
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
	sfv pv[2];
	pv[0].pointer = (char*)key;
	pv[0].size = sizeof(uint32_t);
	pv[1].pointer = NULL;
	pv[1].size = 0;
	svv *v = sv_vbuild(r, pv);
	v->lsn = lsn;
	v->flags = flags;
	sv vv;
	sv_init(&vv, &sv_vif, v, NULL);
	sd_buildadd(b, r, &vv, flags & SVDUP);
	sv_vunref(r, v);
}

static void
sd_read_gt0(void)
{
	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &st_r.r, 1, 0, 0, NULL) == 0);

	int key = 7;
	addv(&b, &st_r.r, 3, 0, &key);
	key = 8;
	addv(&b, &st_r.r, 4, 0, &key);
	key = 9;
	addv(&b, &st_r.r, 5, 0, &key);
	sd_buildend(&b, &st_r.r);

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &st_r.r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &st_r.r, &b, sizeof(sdseal));
	t( rc == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	ssfile f;
	ss_fileinit(&f, &st_r.vfs);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&st_r.r, &f, NULL) == 0 );
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );
	t( sd_indexcommit(&index, &st_r.r, &id, NULL, f.size) == 0 );
	t( sd_writeindex(&st_r.r, &f, NULL, &index) == 0 );
	t( sd_seal(&st_r.r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_vfsmmap(&st_r.vfs, &map, f.fd, f.size, 1) == 0 );

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
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.use_compression = 0,
		.compression_if  = NULL,
		.has             = 0,
		.has_vlsn        = 0,
		.r               = &st_r.r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL);
	t( ss_iteratorhas(&it) == 1 );

	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 9);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_vfsmunmap(&st_r.vfs, &map) == 0 );
	t( ss_vfsunlink(&st_r.vfs, "./0000.db") == 0 );

	sd_indexfree(&index, &st_r.r);
	sd_buildfree(&b, &st_r.r);
	ss_buffree(&xfbuf, &st_r.a);
	ss_buffree(&buf, &st_r.a);
}

static void
sd_read_gt1(void)
{
	ssfile f;
	ss_fileinit(&f, &st_r.vfs);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&st_r.r, &f, NULL) == 0 );

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &st_r.r, 1, 0, 0, NULL) == 0);

	int key = 7;
	addv(&b, &st_r.r, 3, 0, &key);
	key = 8;
	addv(&b, &st_r.r, 4, 0, &key);
	key = 9;
	addv(&b, &st_r.r, 5, 0, &key);
	sd_buildend(&b, &st_r.r);
	uint64_t poff = f.size;
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &st_r.r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &st_r.r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &st_r.r) == 0 );

	t( sd_buildbegin(&b, &st_r.r, 1, 0, 0, NULL) == 0);
	key = 10;
	addv(&b, &st_r.r, 6, 0, &key);
	key = 11;
	addv(&b, &st_r.r, 7, 0, &key);
	key = 13;
	addv(&b, &st_r.r, 8, 0, &key);
	sd_buildend(&b, &st_r.r);
	poff = f.size;
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &st_r.r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &st_r.r) == 0 );

	t( sd_buildbegin(&b, &st_r.r, 1, 0, 0, NULL) == 0);
	key = 15;
	addv(&b, &st_r.r, 9, 0, &key);
	key = 18;
	addv(&b, &st_r.r, 10, 0, &key);
	key = 20;
	addv(&b, &st_r.r, 11, 0, &key);
	sd_buildend(&b, &st_r.r);
	poff = f.size;
	t( sd_writepage(&st_r.r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &st_r.r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &st_r.r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &st_r.r, &id, NULL, f.size) == 0 );
	t( sd_writeindex(&st_r.r, &f, NULL, &index) == 0 );
	t( sd_seal(&st_r.r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_vfsmmap(&st_r.vfs, &map, f.fd, f.size, 1) == 0 );

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
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.use_compression = 0,
		.compression_if  = NULL,
		.has             = 0,
		.has_vlsn        = 0,
		.r               = &st_r.r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL);
	t( ss_iteratorhas(&it) == 1 );

	/* page 0 */
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 9);
	ss_iteratornext(&it);

	/* page 1 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 10);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 11);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 13);
	ss_iteratornext(&it);

	/* page 2 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 15);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 18);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &st_r.r, 0, NULL) == 20);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_vfsmunmap(&st_r.vfs, &map) == 0 );
	t( ss_vfsunlink(&st_r.vfs, "./0000.db") == 0 );

	sd_indexfree(&index, &st_r.r);
	sd_buildfree(&b, &st_r.r);
	ss_buffree(&xfbuf, &st_r.a);
	ss_buffree(&buf, &st_r.a);
}

static void
sd_read_gt0_compression_zstd(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssa aref;
	ss_aopen(&aref, &ss_stda);
	ssvfs vfs;
	ss_vfsinit(&vfs, &ss_stdvfs);
	sfscheme cmp;
	sf_schemeinit(&cmp);
	sffield *field = sf_fieldnew(&a, "key");
	t( sf_fieldoptions(field, &a, "u32,key(0)") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	field = sf_fieldnew(&a, "value");
	t( sf_fieldoptions(field, &a, "string") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	t( sf_schemevalidate(&cmp, &a) == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srstat stat;
	memset(&stat, 0, sizeof(stat));
	srlog log;
	sr_loginit(&log);
	srerror error;
	sr_errorinit(&error, &log);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, NULL, &log, &error, &a, &aref, &vfs, NULL, &seq, SF_RAW,
	        NULL, &cmp, &ij, &stat, crc);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	t( sd_buildend(&b, &r) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, sizeof(sdseal));
	t( rc == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	ssfile f;
	ss_fileinit(&f, &vfs);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );
	t( sd_writepage(&r, &f, NULL, &b) == 0 );
	t( sd_indexcommit(&index, &r, &id, NULL, f.size) == 0 );
	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	t( sd_buildcommit(&b, &r) == 0 );

	ssmmap map;
	t( ss_vfsmmap(&st_r.vfs, &map, f.fd, f.size, 1) == 0 );

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
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.use_compression = 1,
		.compression_if  = &ss_zstdfilter,
		.has             = 0,
		.has_vlsn        = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL);
	t( ss_iteratorhas(&it) == 1 );

	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 9);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_vfsmunmap(&st_r.vfs, &map) == 0 );
	t( ss_vfsunlink(&vfs, "./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);

	ss_buffree(&xfbuf, &a);
	ss_buffree(&buf, &a);
	sf_schemefree(&cmp, &a);
}

static void
sd_read_gt0_compression_lz4(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssa aref;
	ss_aopen(&aref, &ss_stda);
	ssvfs vfs;
	ss_vfsinit(&vfs, &ss_stdvfs);
	sfscheme cmp;
	sf_schemeinit(&cmp);
	sffield *field = sf_fieldnew(&a, "key");
	t( sf_fieldoptions(field, &a, "u32,key(0)") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	field = sf_fieldnew(&a, "value");
	t( sf_fieldoptions(field, &a, "string") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	t( sf_schemevalidate(&cmp, &a) == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srstat stat;
	memset(&stat, 0, sizeof(stat));
	srlog log;
	sr_loginit(&log);
	srerror error;
	sr_errorinit(&error, &log);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, NULL, &log, &error, &a, &aref, &vfs, NULL, &seq, SF_RAW,
	        NULL, &cmp, &ij, &stat, crc);

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_lz4filter) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	t( sd_buildend(&b, &r) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, sizeof(sdseal));
	t( rc == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));

	t( sd_indexcommit(&index, &r, &id, NULL, 0) == 0 );

	ssfile f;
	ss_fileinit(&f, &vfs);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );
	t( sd_writepage(&r, &f, NULL, &b) == 0 );
	t( sd_indexcommit(&index, &r, &id, NULL, f.size) == 0 );
	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_vfsmmap(&st_r.vfs, &map, f.fd, f.size, 1) == 0 );

	t( sd_buildcommit(&b, &r) == 0 );

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
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.use_compression = 1,
		.compression_if  = &ss_lz4filter,
		.has             = 0,
		.has_vlsn        = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL);
	t( ss_iteratorhas(&it) == 1 );

	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 9);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_vfsmunmap(&st_r.vfs, &map) == 0 );
	t( ss_vfsunlink(&vfs, "./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);

	ss_buffree(&xfbuf, &a);
	ss_buffree(&buf, &a);
	sf_schemefree(&cmp, &a);
}

static void
sd_read_gt1_compression_zstd(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssa aref;
	ss_aopen(&aref, &ss_stda);
	ssvfs vfs;
	ss_vfsinit(&vfs, &ss_stdvfs);
	sfscheme cmp;
	sf_schemeinit(&cmp);
	sffield *field = sf_fieldnew(&a, "key");
	t( sf_fieldoptions(field, &a, "u32,key(0)") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	field = sf_fieldnew(&a, "value");
	t( sf_fieldoptions(field, &a, "string") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	t( sf_schemevalidate(&cmp, &a) == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srstat stat;
	memset(&stat, 0, sizeof(stat));
	srlog log;
	sr_loginit(&log);
	srerror error;
	sr_errorinit(&error, &log);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, NULL, &log, &error, &a, &aref, &vfs, NULL, &seq, SF_RAW,
	        NULL, &cmp, &ij, &stat, crc);

	ssfile f;
	ss_fileinit(&f, &vfs);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b, &r);
	uint64_t poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );
	sd_buildreset(&b, &r);

	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);
	key = 10;
	addv(&b, &r, 6, 0, &key);
	key = 11;
	addv(&b, &r, 7, 0, &key);
	key = 13;
	addv(&b, &r, 8, 0, &key);
	sd_buildend(&b, &r);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );
	sd_buildreset(&b, &r);

	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_zstdfilter) == 0);
	key = 15;
	addv(&b, &r, 9, 0, &key);
	key = 18;
	addv(&b, &r, 10, 0, &key);
	key = 20;
	addv(&b, &r, 11, 0, &key);
	sd_buildend(&b, &r);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id, NULL, f.size) == 0 );

	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_vfsmmap(&st_r.vfs, &map, f.fd, f.size, 1) == 0 );

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
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.use_compression = 1,
		.compression_if  = &ss_zstdfilter,
		.has             = 0,
		.has_vlsn        = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL);
	t( ss_iteratorhas(&it) == 1 );

	/* page 0 */
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 9);
	ss_iteratornext(&it);

	/* page 1 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 10);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 11);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 13);
	ss_iteratornext(&it);

	/* page 2 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 15);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 18);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 20);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_vfsmunmap(&st_r.vfs, &map) == 0 );
	t( ss_vfsunlink(&vfs, "./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sf_schemefree(&cmp, &a);
}

static void
sd_read_gt1_compression_lz4(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssa aref;
	ss_aopen(&aref, &ss_stda);
	ssvfs vfs;
	ss_vfsinit(&vfs, &ss_stdvfs);
	sfscheme cmp;
	sf_schemeinit(&cmp);
	sffield *field = sf_fieldnew(&a, "key");
	t( sf_fieldoptions(field, &a, "u32,key(0)") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	field = sf_fieldnew(&a, "value");
	t( sf_fieldoptions(field, &a, "string") == 0 );
	t( sf_schemeadd(&cmp, &a, field) == 0 );
	t( sf_schemevalidate(&cmp, &a) == 0 );
	ssinjection ij;
	memset(&ij, 0, sizeof(ij));
	srstat stat;
	memset(&stat, 0, sizeof(stat));
	srlog log;
	sr_loginit(&log);
	srerror error;
	sr_errorinit(&error, &log);
	srseq seq;
	sr_seqinit(&seq);
	sscrcf crc = ss_crc32c_function();
	sr r;
	sr_init(&r, NULL, &log, &error, &a, &aref, &vfs, NULL, &seq, SF_RAW,
	        NULL, &cmp, &ij, &stat, crc);

	ssfile f;
	ss_fileinit(&f, &vfs);
	t( ss_filenew(&f, "./0000.db") == 0 );
	t( sd_writeseal(&r, &f, NULL) == 0 );

	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_lz4filter) == 0);

	int key = 7;
	addv(&b, &r, 3, 0, &key);
	key = 8;
	addv(&b, &r, 4, 0, &key);
	key = 9;
	addv(&b, &r, 5, 0, &key);
	sd_buildend(&b, &r);
	uint64_t poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	sdindex index;
	sd_indexinit(&index);
	t( sd_indexbegin(&index, &r) == 0 );

	int rc;
	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );
	sd_buildreset(&b, &r);

	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_lz4filter) == 0);
	key = 10;
	addv(&b, &r, 6, 0, &key);
	key = 11;
	addv(&b, &r, 7, 0, &key);
	key = 13;
	addv(&b, &r, 8, 0, &key);
	sd_buildend(&b, &r);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );
	sd_buildreset(&b, &r);

	t( sd_buildbegin(&b, &r, 1, 0, 1, &ss_lz4filter) == 0);
	key = 15;
	addv(&b, &r, 9, 0, &key);
	key = 18;
	addv(&b, &r, 10, 0, &key);
	key = 20;
	addv(&b, &r, 11, 0, &key);
	sd_buildend(&b, &r);
	poff = f.size;
	t( sd_writepage(&r, &f, NULL, &b) == 0 );

	rc = sd_indexadd(&index, &r, &b, poff);
	t( rc == 0 );
	t( sd_buildcommit(&b, &r) == 0 );

	sdid id;
	memset(&id, 0, sizeof(id));
	t( sd_indexcommit(&index, &r, &id, NULL, f.size) == 0 );

	t( sd_writeindex(&r, &f, NULL, &index) == 0 );
	t( sd_seal(&r, &f, NULL, &index, 0) == 0 );

	ssmmap map;
	t( ss_vfsmmap(&st_r.vfs, &map, f.fd, f.size, 1) == 0 );

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
		.mmap            = &map,
		.memory          = NULL,
		.file            = NULL,
		.o               = SS_GT,
		.use_memory      = 0,
		.use_mmap        = 1,
		.use_mmap_copy   = 0,
		.use_compression = 1,
		.compression_if  = &ss_lz4filter,
		.has             = 0,
		.has_vlsn        = 0,
		.r               = &r
	};

	ssiter it;
	ss_iterinit(sd_read, &it);
	ss_iteropen(sd_read, &it, &arg, NULL);
	t( ss_iteratorhas(&it) == 1 );

	/* page 0 */
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 7);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 8);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 9);
	ss_iteratornext(&it);

	/* page 1 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 10);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 11);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 13);
	ss_iteratornext(&it);

	/* page 2 */
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 15);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 18);
	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( *(int*)sv_field(v, &r, 0, NULL) == 20);
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) == 0 );
	ss_iteratorclose(&it);

	ss_fileclose(&f);
	t( ss_vfsmunmap(&st_r.vfs, &map) == 0 );
	t( ss_vfsunlink(&vfs, "./0000.db") == 0 );

	sd_indexfree(&index, &r);
	sd_buildfree(&b, &r);
	ss_buffree(&buf, &a);
	ss_buffree(&xfbuf, &a);
	sf_schemefree(&cmp, &a);
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
