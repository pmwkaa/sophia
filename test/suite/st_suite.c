
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
	s->current = 0;
	s->stop_plan = 0;
	s->stop_group = 0;
	s->stop_test = 0;
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

static int
st_suiterun_group(stsuite *s, stplan *plan, stgroup *group)
{
	sslist *i;
	ss_listforeach(&group->test, i)
	{
		int j = 0;
		if (s->current < s->position) {
			while (j < plan->scene_count) {
				s->current++;
				j++;
			}
			continue;
		}
		sttest *test = sscast(i, sttest, link);
		st_r.env         = NULL;
		st_r.db          = NULL;
		st_r.phase_scene = 0;
		st_r.phase       = 0;
		st_r.test        = test;
		st_r.group       = group;
		st_r.plan        = plan;
		printf("[%08d] ", s->current);
		printf("%s.%s.%s", plan->name, group->name, test->name);
		fflush(NULL);
		j = 0;
		while (j < plan->scene_count) {
			stscene *scene = &plan->scene[j];
			scene->function(scene);
			s->current++;
			j++;
		}
		if (s->stop_test)
			return 1;
	}
	return 0;
}

void st_suiteset(stsuite *s, int position,
                 int stop_plan,
                 int stop_group,
                 int stop_test)
{
	s->position   = position;
	s->stop_plan  = stop_plan;
	s->stop_group = stop_group;
	s->stop_test  = stop_test;
}

void st_suiterun(stsuite *s)
{
	s->current = 0;

	sslist *i;
	ss_listforeach(&s->plan, i) {
		stplan *plan = sscast(i, stplan, link);
		sslist *j;
		ss_listforeach(&plan->group, j) {
			stgroup *group = sscast(j, stgroup, link);
			do {
				int stop = st_suiterun_group(s, plan, group);
				if (stop)
					return;
			} while (st_plannext(plan));
			st_planreset(plan);
			if (s->stop_group && (s->current > s->position))
				return;
		}
		if (s->current > s->position) {
			if (s->stop_plan)
				return;
			printf("\n");
		}
	}
}
