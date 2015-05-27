
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libss.h>
#include <libst.h>
#include <sophia.h>
#include <assert.h>

stscene *st_scene(char *name, stscenef function, int statemax)
{
	stscene *scene = malloc(sizeof(*scene));
	assert( scene != NULL );
	scene->name = name;
	scene->state = 0;
	scene->statemax = statemax;
	scene->function = function;
	ss_listinit(&scene->link);
	return scene;
}

stplan *st_plan(char *name)
{
	stplan *plan = malloc(sizeof(*plan));
	assert( plan != NULL );
	plan->name = name;
	plan->scene_count = 0;
	plan->group_count = 0;
	ss_listinit(&plan->group);
	ss_listinit(&plan->link);
	return plan;
}

stgroup *st_group(char *name)
{
	stgroup *group = malloc(sizeof(*group));
	assert( group != NULL );
	group->name = name;
	group->count = 0;
	ss_listinit(&group->test);
	ss_listinit(&group->link);
	return group;
}

sttest *st_test(char *name, stf function)
{
	sttest *test = malloc(sizeof(*test));
	assert( test != NULL );
	test->name = name;
	test->function = function;
	ss_listinit(&test->link);
	return test;
}

static inline void
st_testfree(sttest *test)
{
	free(test);
}

static void
st_groupfree(stgroup *group)
{
	sslist *i, *n;
	ss_listforeach_safe(&group->test, i, n) {
		sttest *test = sscast(i, sttest, link);
		st_testfree(test);
	}
	free(group);
}

static void
st_planfree(stplan *plan)
{
	sslist *i, *n;
	ss_listforeach_safe(&plan->group, i, n) {
		stgroup *group = sscast(i, stgroup, link);
		st_groupfree(group);
	}
	free(plan);
}

static inline void
st_scenefree(stscene *scene)
{
	free(scene);
}

void st_init(st *s, char *sophiadir, char *backupdir,
             char *dir,
             char *logdir)
{
	s->sophiadir = sophiadir;
	s->backupdir = backupdir;
	s->dir = dir;
	s->logdir = logdir;
	ss_listinit(&s->scene);
	ss_listinit(&s->plan);
	s->scene_count = 0;
	s->plan_count = 0;
	s->stat_stmt = 0;
	s->stat_test = 0;
}

void st_free(st *s)
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

void st_add(st *s, stplan *p)
{
	ss_listappend(&s->plan, &p->link);
	s->plan_count++;
}

stscene *st_sceneof(st *s, char *name)
{
	sslist *i;
	ss_listforeach(&s->scene, i) {
		stscene *scene = sscast(i, stscene, link);
		if (strcmp(scene->name, name) == 0)
			return scene;
	}
	return NULL;
}

void st_addscene(st *s, stscene *scene)
{
	ss_listappend(&s->scene, &scene->link);
	s->scene_count++;
}

void st_planadd(stplan *p, stgroup *g)
{
	ss_listappend(&p->group, &g->link);
	p->group_count++;
}

void st_planscene(stplan *p, stscene *s)
{
	assert( s != NULL );
	int id = p->scene_count;
	p->scene[id] = *s;
	p->scene_count++;
}

static inline void
st_planreset(stplan *p)
{
	int i = 0;
	while (i < p->scene_count) {
		p->scene[i].state = 0;
		i++;
	}
}

void st_groupadd(stgroup *g, sttest *t)
{
	ss_listappend(&g->test, &t->link);
	g->count++;
}

void st_transaction(stc *cx)
{
	if (cx->commit)
		cx->commit(cx);
}

void st_error(stc *cx)
{
	if (cx->env == NULL)
		return;
	void *c = sp_ctl(cx->env);
	void *o = sp_get(c, "sophia.error");
	char *value = sp_get(o, "value", NULL);
	if (value)
		printf("%s\n", value);
	sp_destroy(o);
}

static void
st_rungroup(st *s, stplan *plan, stgroup *group)
{
	sslist *i;
	ss_listforeach(&group->test, i)
	{
		sttest *test = sscast(i, sttest, link);
		stc context = {
			.env         = NULL,
			.db          = NULL,
			.phase_scene = 0,
			.phase       = 0,
			.commit      = NULL,
			.test        = test,
			.group       = group,
			.plan        = plan,
			.suite       = s
		};
		printf("%s.%s", group->name, test->name);
		fflush(NULL);
		int i = 0;
		while (i < plan->scene_count) {
			stscene *scene = &plan->scene[i];
			scene->function(scene, &context);
			i++;
		}
	}
}

static inline int
st_plannext(stplan *plan)
{
	int i = plan->scene_count - 1;
	while (i >= 0) {
		stscene *scene = &plan->scene[i];
		scene->state++;
		if (scene->state == scene->statemax)
			scene->state = 0;
		else
			return 1;
		i--;
	}
	return 0;
}

static inline void
st_runplan(st *s, stplan *plan)
{
	printf("\n<%s>\n", plan->name);
	sslist *i;
	ss_listforeach(&plan->group, i) {
		stgroup *group = sscast(i, stgroup, link);
		do {
			st_rungroup(s, plan, group);
		} while (st_plannext(plan));
		st_planreset(plan);
	}
}

void st_run(st *s)
{
	printf("sophia test-suite.\n");

	sslist *i;
	ss_listforeach(&s->plan, i) {
		stplan *plan = sscast(i, stplan, link);
		st_runplan(s, plan);
	}

	printf("\n");
	printf("tests passed: %d\n", s->stat_test);
	printf("statements passed: %d\n", s->stat_stmt);
	printf("\n");
	printf("complete.\n");
}
