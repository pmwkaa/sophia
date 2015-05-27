
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
#include <libsl.h>
#include <libsd.h>
#include <libst.h>
#include <sophia.h>

static void
sspager_test0(stc *cx)
{
	sspager p;
	ss_pagerinit(&p, 3, 1024);

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

stgroup *sspager_group(void)
{
	stgroup *group = st_group("sspager");
	st_groupadd(group, st_test("test0", sspager_test0));
	return group;
}
