
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

void st_init(stconf *c)
{
	memset(&st_r, 0, sizeof(st_r));
	st_r.verbose = c->verbose;
	st_r.conf = c;
	st_suiteinit(&st_r.suite);
}

void st_free(void)
{
	st_suitefree(&st_r.suite);
}

void st_phase(void)
{
	st_phase_commit();
}

void st_run(void)
{
	printf("\n");
	printf("sophia test-suite.\n");

	st_suiterun(&st_r.suite);

	printf("\n");
	printf("tests passed: %d\n", st_r.stat_test);
	printf("statements passed: %d\n", st_r.stat_stmt);
	printf("\n");
	printf("complete.\n");
}
