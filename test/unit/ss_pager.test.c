
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
ss_pager_test0(void)
{
	sspager p;
	ss_pagerinit(&p, &st_r.vfs, 3, 1024);

	t( ss_pageradd(&p) == 0 );

	void *a = ss_pagerpop(&p);
	t( a != NULL );
	void *b = ss_pagerpop(&p);
	t( b != NULL );
	void *c = ss_pagerpop(&p);
	t( c != NULL );
	void *d = ss_pagerpop(&p);
	t( c != NULL );
	void *e = ss_pagerpop(&p);
	t( e != NULL );
	void *f = ss_pagerpop(&p);
	t( f != NULL );

	ss_pagerpush(&p, f);
	ss_pagerpush(&p, e);
	ss_pagerpush(&p, d);
	ss_pagerpush(&p, c);
	ss_pagerpush(&p, b);
	ss_pagerpush(&p, a);

	a = ss_pagerpop(&p);
	t( a != NULL );
	b = ss_pagerpop(&p);
	t( b != NULL );
	c = ss_pagerpop(&p);
	t( c != NULL );
	d = ss_pagerpop(&p);
	t( c != NULL );
	e = ss_pagerpop(&p);
	t( e != NULL );

	ss_pagerfree(&p);
}

stgroup *ss_pager_group(void)
{
	stgroup *group = st_group("sspager");
	st_groupadd(group, st_test("test0", ss_pager_test0));
	return group;
}
