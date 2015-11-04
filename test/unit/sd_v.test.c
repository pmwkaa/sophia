
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
	svv *v = sv_vbuild(r, &pv, 1, (char*)key, sizeof(uint32_t));
	v->lsn = lsn;
	v->flags = flags;
	sv vv;
	sv_init(&vv, &sv_vif, v, NULL);
	sd_buildadd(b, r, &vv, flags & SVDUP);
	sv_vfree(r, v);
}

static void
sd_v_test(void)
{
	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &st_r.r, 1, 0, 0, NULL) == 0);
	int i = 7;
	int j = 8;
	addv(&b, &st_r.r, 3, 0, &i);
	addv(&b, &st_r.r, 4, 0, &j);
	sd_buildend(&b, &st_r.r);

	ssbuf buf;
	ss_bufinit(&buf);
	ssbuf xfbuf;
	ss_bufinit(&xfbuf);
	t( ss_bufensure(&xfbuf, &st_r.a, 1024) == 0 );

	t( sd_commitpage(&b, &st_r.r, &buf) == 0 );
	sdpageheader *h = (sdpageheader*)buf.s;
	sdpage page;
	sd_pageinit(&page, h);

	ssiter it;
	ss_iterinit(sd_pageiter, &it);
	ss_iteropen(sd_pageiter, &it, &st_r.r, &xfbuf, &page, SS_GTE, NULL, 0);
	t( ss_iteratorhas(&it) != 0 );
	sv *v = ss_iteratorof(&it);
	t( v != NULL );

	t( *(int*)sv_key(v, &st_r.r, 0) == i );
	t( sv_lsn(v) == 3 );
	t( sv_flags(v) == 0 );
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );

	v = ss_iteratorof(&it);
	t( v != NULL );
	
	t( *(int*)sv_key(v, &st_r.r, 0) == j );
	t( sv_lsn(v) == 4 );
	t( sv_flags(v) == 0 );

	ss_iteratornext(&it);
	v = ss_iteratorof(&it);
	t( v == NULL );

	sd_buildfree(&b, &st_r.r);
	ss_buffree(&buf, &st_r.a);
	ss_buffree(&xfbuf, &st_r.a);
}

stgroup *sd_v_group(void)
{
	stgroup *group = st_group("sdv");
	st_groupadd(group, st_test("test", sd_v_test));
	return group;
}
