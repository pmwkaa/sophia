
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
	sfv pv[8];
	memset(pv, 0, sizeof(pv));
	pv[0].pointer = (char*)key;
	pv[0].size = sizeof(uint32_t);
	pv[1].pointer = NULL;
	pv[1].size = 0;
	svv *v = sv_vbuild(r, pv);
	sf_lsnset(r->scheme, sv_vpointer(v), lsn);
	sf_flagsset(r->scheme, sv_vpointer(v), flags);
	sd_buildadd(b, r, sv_vpointer(v), flags & SVDUP);
	sv_vunref(r, v);
}

static void
sd_v_test(void)
{
	sdbuild b;
	sd_buildinit(&b);
	t( sd_buildbegin(&b, &st_r.r, 1, 0, NULL) == 0);
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
	ss_iteropen(sd_pageiter, &it, &st_r.r, &page, SS_GTE, NULL);
	t( ss_iteratorhas(&it) != 0 );
	char *v = ss_iteratorof(&it);
	t( v != NULL );

	t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == i );
	t( sf_lsn(st_r.r.scheme, v) == 3 );
	t( sf_flags(st_r.r.scheme, v) == 0 );
	ss_iteratornext(&it);
	t( ss_iteratorhas(&it) != 0 );

	v = ss_iteratorof(&it);
	t( v != NULL );
	
	t( *(int*)sf_field(st_r.r.scheme, 0, v, &st_r.size) == j );
	t( sf_lsn(st_r.r.scheme, v) == 4 );
	t( sf_flags(st_r.r.scheme, v) == 0 );

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
