
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libst.h>
#include <sophia.h>

static void
sraslab_reuse(stc *cx)
{
	srpager p;
	sr_pagerinit(&p, 3, 1024);

	sra slab;
	t( sr_allocopen(&slab, &sr_aslab, &p, 32) == 0 );

	void *alloc0[1000];
	void *alloc1[1000];
	memset(alloc0, 0, sizeof(alloc0));
	memset(alloc1, 0, sizeof(alloc1));

	int i = 0;
	while (i < 1000) {
		alloc0[i] = sr_malloc(&slab, 0);
		t( alloc0[i] != NULL );
		i++;
	}
	i--;
	int pools = p.pools;
	while (i >= 0) {
		sr_free(&slab, alloc0[i]);
		i--;
	}
	t( p.pools == pools );

	i++;
	while (i < 1000) {
		alloc1[i] = sr_malloc(&slab, 0);
		t( alloc0[i] == alloc1[i] );
		i++;
	}
	t( p.pools == pools );

	sr_allocclose(&slab);
	sr_pagerfree(&p);
}

stgroup *sraslab_group(void)
{
	stgroup *group = st_group("sraslab");
	st_groupadd(group, st_test("reuse", sraslab_reuse));
	return group;
}
