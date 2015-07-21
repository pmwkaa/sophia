
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
	s->stop_plan = 0;
	s->stop_group = 0;
	s->stop_test = 0;
	s->total = 0;
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
	p->id = s->plan_count++;
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

void st_suiteset(stsuite *s, int stop_plan, int stop_group,
                 int stop_test)
{
	s->stop_plan  = stop_plan;
	s->stop_group = stop_group;
	s->stop_test  = stop_test;
}

static int
st_suiterun_set(stsuite *s, char *id,
                sslist **pos_plan,
                sslist **pos_group,
                sslist **pos_test)
{
	char sz[64];
	snprintf(sz, sizeof(sz), "%s", id);
	char *p = strtok(sz, ":");
	if (p == NULL)
		return -1;
	int plan_id = atoi(p);
	p = strtok(NULL, ":");
	if (p == NULL)
		return -1;
	int group_id = atoi(p);
	p = strtok(NULL, ":");
	if (p == NULL)
		return -1;
	int test_id = atoi(p);
	p = strtok(NULL, ":");
	if (p == NULL)
		return -1;

	stplan *plan;
	sslist *i;
	ss_listforeach(&s->plan, i) {
		plan = sscast(i, stplan, link);
		if (plan->id == plan_id)
			break;
	}
	if (i == &s->plan)
		return -1;
	*pos_plan = i;

	stgroup *group;
	ss_listforeach(&plan->group, i) {
		group = sscast(i, stgroup, link);
		if (group->id == group_id)
			break;
	}
	if (i == &plan->group)
		return -1;
	*pos_group = i;

	sttest *test;
	ss_listforeach(&group->test, i) {
		test = sscast(i, sttest, link);
		if (test->id == test_id)
			break;
	}
	if (i == &group->test)
		return -1;
	*pos_test = i;

	int j = 0;
	while (j < plan->scene_count) {
		stscene *scene = &plan->scene[j];
		if (scene->statemax > 1) {
			if (*p == 0)
				return -1;
			if (! isdigit(*p))
				return -1;
			scene->state = *p - '0';
			p++;
		}
		j++;
	}
	return 0;
}

void st_suiterun(stsuite *s, char *id)
{
	sslist *i = NULL;
	sslist *j = NULL;
	sslist *k = NULL;
	if (id) {
		int rc = st_suiterun_set(s, id, &i, &j, &k);
		if (rc == -1) {
			fprintf(st_r.output, "error: bad test id\n");
			return;
		}
	}

	if (i == NULL)
		i = s->plan.next;
	ss_listforeach_continue(&s->plan, i)
	{
		stplan *plan = sscast(i, stplan, link);
		if (j == NULL)
			j = plan->group.next;
		ss_listforeach_continue(&plan->group, j)
		{
			stgroup *group = sscast(j, stgroup, link);
			do {
				if (k == NULL)
					k = group->test.next;
				ss_listforeach_continue(&group->test, k)
				{
					sttest *test = sscast(k, sttest, link);
					st_r.env   = NULL;
					st_r.db    = NULL;
					st_r.phase_compaction_scene = 0;
					st_r.phase_compaction = 0;
					st_r.test  = test;
					st_r.group = group;
					st_r.plan  = plan;

					int percent = (st_r.stat_test * 100.0) / st_r.suite.total;
					fprintf(st_r.output, "[%02d%%] ", percent);

					int g = 0;
					fprintf(st_r.output, "(%02d:%02d:%02d:", plan->id, group->id, test->id);
					while (g < plan->scene_count) {
						stscene *scene = &plan->scene[g];
						if (scene->statemax > 1)
							fprintf(st_r.output, "%d", scene->state);
						g++;
					}
					fprintf(st_r.output, ") %s.%s.%s", plan->name, group->name, test->name);
					fflush(st_r.output);

					g = 0;
					while (g < plan->scene_count) {
						stscene *scene = &plan->scene[g];
						scene->function(scene);
						g++;
					}
					if (s->stop_test)
						return;
				}
				k = NULL;
			} while (st_plannext(plan));

			st_planreset(plan);
			if (s->stop_group)
				return;
		}
		j = NULL;
		if (s->stop_plan)
			return;
		fprintf(st_r.output, "\n");
	}
}
