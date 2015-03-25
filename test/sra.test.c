
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>

static void
sra_malloc(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	void *buf = sr_malloc(&a, 123);
	t( buf != NULL );
	sr_free(&a, buf);
	sr_aclose(&a);
}

static void
sra_realloc(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);
	void *buf = sr_malloc(&a, 123);
	t( buf != NULL );
	buf = sr_realloc(&a, buf, 321);
	t( buf != NULL );
	sr_free(&a, buf);
	sr_aclose(&a);
}

stgroup *sra_group(void)
{
	stgroup *group = st_group("sra");
	st_groupadd(group, st_test("malloc", sra_malloc));
	st_groupadd(group, st_test("realloc", sra_realloc));
	return group;
}
