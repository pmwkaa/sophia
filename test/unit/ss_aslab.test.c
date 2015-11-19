
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
#include <libso.h>
#include <libst.h>

static void
ss_aslab_reuse(void)
{
	sspager p;
	ss_pagerinit(&p, &st_r.vfs, 3, 1024);

	ssa slab;
	t( ss_aopen(&slab, &ss_slaba, &p, 32) == 0 );

	void *alloc0[1000];
	void *alloc1[1000];
	memset(alloc0, 0, sizeof(alloc0));
	memset(alloc1, 0, sizeof(alloc1));

	int i = 0;
	while (i < 1000) {
		alloc0[i] = ss_malloc(&slab, 0);
		t( alloc0[i] != NULL );
		i++;
	}
	i--;
	int pools = p.pools;
	while (i >= 0) {
		ss_free(&slab, alloc0[i]);
		i--;
	}
	t( p.pools == pools );

	i++;
	while (i < 1000) {
		alloc1[i] = ss_malloc(&slab, 0);
		t( alloc0[i] == alloc1[i] );
		i++;
	}
	t( p.pools == pools );

	ss_aclose(&slab);
	ss_pagerfree(&p);
}

stgroup *ss_aslab_group(void)
{
	stgroup *group = st_group("ssaslab");
	st_groupadd(group, st_test("reuse", ss_aslab_reuse));
	return group;
}
