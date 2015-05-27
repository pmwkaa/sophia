
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
#include <libst.h>

static void
ssa_malloc(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	void *buf = ss_malloc(&a, 123);
	t( buf != NULL );
	ss_free(&a, buf);
	ss_aclose(&a);
}

static void
ssa_realloc(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	void *buf = ss_malloc(&a, 123);
	t( buf != NULL );
	buf = ss_realloc(&a, buf, 321);
	t( buf != NULL );
	ss_free(&a, buf);
	ss_aclose(&a);
}

stgroup *ssa_group(void)
{
	stgroup *group = st_group("sra");
	st_groupadd(group, st_test("malloc", ssa_malloc));
	st_groupadd(group, st_test("realloc", ssa_realloc));
	return group;
}
