
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

extern void workflow_test(char*);

static void
io_test(void)
{
	workflow_test("debug.error_injection.io");
}

stgroup *io_group(void)
{
	stgroup *group = st_group("io");
	st_groupadd(group, st_test("test", io_test));
	return group;
}
