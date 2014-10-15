
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
srpager_test0(stc *cx)
{
	srpager p;
	sr_pagerinit(&p, 3, 1024);

	t( sr_pageradd(&p) == 0 );

	void *a = sr_pagerpop(&p);
	t( a != NULL );
	void *b = sr_pagerpop(&p);
	t( b != NULL );
	void *c = sr_pagerpop(&p);
	t( c != NULL );
	void *d = sr_pagerpop(&p);
	t( c != NULL );
	void *e = sr_pagerpop(&p);
	t( e != NULL );
	void *f = sr_pagerpop(&p);
	t( f != NULL );

	sr_pagerpush(&p, f);
	sr_pagerpush(&p, e);
	sr_pagerpush(&p, d);
	sr_pagerpush(&p, c);
	sr_pagerpush(&p, b);
	sr_pagerpush(&p, a);

	a = sr_pagerpop(&p);
	t( a != NULL );
	b = sr_pagerpop(&p);
	t( b != NULL );
	c = sr_pagerpop(&p);
	t( c != NULL );
	d = sr_pagerpop(&p);
	t( c != NULL );
	e = sr_pagerpop(&p);
	t( e != NULL );

	sr_pagerfree(&p);
}

stgroup *srpager_group(void)
{
	stgroup *group = st_group("srpager");
	st_groupadd(group, st_test("test0", srpager_test0));
	return group;
}
