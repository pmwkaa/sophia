
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

void st_suiteinit(stsuite *s)
{
	ss_listinit(&s->plan);
	ss_listinit(&s->scene);
	s->plan_count  = 0;
	s->scene_count = 0;
}

void st_suitefree(stsuite *s)
{
	sslist *i, *n;
	ss_listforeach_safe(&s->plan, i, n) {
		stplan *plan = sscast(i, stplan, link);
		st_planfree(plan);
	}
	ss_listforeach_safe(&s->scene, i, n) {
		stscene *scene = sscast(i, stscene, link);
		st_scenefree(scene);
	}
}

void st_suiteadd(stsuite *s, stplan *p)
{
	ss_listappend(&s->plan, &p->link);
	s->plan_count++;
}

stscene *st_suitescene_of(stsuite *s, char *name)
{
	sslist *i;
	ss_listforeach(&s->scene, i) {
		stscene *scene = sscast(i, stscene, link);
		if (strcmp(scene->name, name) == 0)
			return scene;
	}
	return NULL;
}

void st_suiteadd_scene(stsuite *s, stscene *scene)
{
	ss_listappend(&s->scene, &scene->link);
	s->scene_count++;
}

static void
st_suiterun_group(stsuite *s, stplan *plan, stgroup *group)
{
	sslist *i;
	ss_listforeach(&group->test, i)
	{
		sttest *test = sscast(i, sttest, link);
		st_r.env         = NULL;
		st_r.db          = NULL;
		st_r.phase_scene = 0;
		st_r.phase       = 0;
		st_r.test        = test;
		st_r.group       = group;
		st_r.plan        = plan;

		int i = 0;
		if (! st_r.verbose) {
			printf("(");
			while (i < plan->scene_count) {
				stscene *scene = &plan->scene[i];
				printf("%s%d", (i > 0 ? "." : ""), scene->state);
				i++;
			}
			printf(") ");
			fflush(NULL);
		}
		printf("%s.%s.%s", plan->name, group->name, test->name);
		fflush(NULL);
		i = 0;
		while (i < plan->scene_count) {
			stscene *scene = &plan->scene[i];
			scene->function(scene);
			i++;
		}
	}
}

static inline void
st_suiterun_plan(stsuite *s, stplan *plan)
{
	printf("\n");
	sslist *i;
	ss_listforeach(&plan->group, i) {
		stgroup *group = sscast(i, stgroup, link);
		do {
			st_suiterun_group(s, plan, group);
		} while (st_plannext(plan));
		st_planreset(plan);
	}
}

void st_suiterun(stsuite *s)
{
	sslist *i;
	ss_listforeach(&s->plan, i) {
		stplan *plan = sscast(i, stplan, link);
		st_suiterun_plan(s, plan);
	}
}
