
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
ssa_malloc(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	void *buf = ss_malloc(&a, 123);
	t( buf != NULL );
	ss_free(&a, buf);
	ss_aclose(&a);
}

static void
ssa_realloc(void)
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

stgroup *ss_a_group(void)
{
	stgroup *group = st_group("ssa");
	st_groupadd(group, st_test("malloc", ssa_malloc));
	st_groupadd(group, st_test("realloc", ssa_realloc));
	return group;
}
