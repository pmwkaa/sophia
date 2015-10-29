
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
oom_test(void)
{
	workflow_test("debug.error_injection.oom");
}

stgroup *oom_group(void)
{
	stgroup *group = st_group("oom");
	st_groupadd(group, st_test("test", oom_test));
	return group;
}
