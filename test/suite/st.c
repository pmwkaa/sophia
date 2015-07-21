
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
	st_r.output = stdout;
	st_r.report = -1;
	st_suiteinit(&st_r.suite);
}

void st_free(void)
{
	if (st_r.output != stdout)
		fclose(st_r.output);
	st_suitefree(&st_r.suite);
}

void st_phase(void)
{
	st_phase_commit();
}

static inline void
st_suitetotal(stsuite *s)
{
	sslist *i, *j;
	ss_listforeach(&s->plan, i)
	{
		stplan *plan = sscast(i, stplan, link);
		int combinations = 1;
		int g = 0;
		int tests = 0;
		while (g < plan->scene_count) {
			stscene *scene = &plan->scene[g];
			if (scene->function == st_scene_test)
				tests++;
			combinations *= scene->statemax;
			g++;
		}
		ss_listforeach(&plan->group, j)
		{
			stgroup *group = sscast(j, stgroup, link);
			s->total += tests * (combinations * group->count);
		}
	}
}

static inline void
st_banner(FILE *f)
{
	fprintf(f,  "\n");
	fprintf(f, "sophia test-suite.\n\n");
	fprintf(f, "expected tests: %d\n", st_r.suite.total);
	fprintf(f, "\n");
}

static inline void
st_complete(FILE *f)
{
	fprintf(f, "\n");
	fprintf(f, "tests passed: %d\n", st_r.stat_test);
	fprintf(f, "statements passed: %d\n", st_r.stat_stmt);
	fprintf(f, "\n");
	fprintf(f, "complete.\n");
}

void st_run(void)
{
	if (st_r.conf->logfile) {
		FILE *f = fopen(st_r.conf->logfile, "w");
		if (f == NULL) {
			printf("error: failed to create logfile '%s'",
			       st_r.conf->logfile);
			return;
		}
		st_r.output = f;
	}
	st_suitetotal(&st_r.suite);

	st_banner(st_r.output);
	if (st_r.output != stdout && st_r.conf->report)
		st_banner(stdout);

	st_suiteset(&st_r.suite, st_r.conf->stop_plan,
	            st_r.conf->stop_group,
	            st_r.conf->stop_test);
	st_suiterun(&st_r.suite, st_r.conf->id);

	st_complete(st_r.output);
	if (st_r.output != stdout && st_r.conf->report)
		st_complete(stdout);
}

void st_seedset(int seed)
{
	srand(seed);
}

int st_seed(void)
{
	return rand();
}
