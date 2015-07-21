#ifndef ST_PLAN_H_
#define ST_PLAN_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stplan stplan;

struct stplan {
	char *name;
	int id;
	sslist group;
	stscene scene[100];
	int group_count;
	int scene_count;
	sslist link;
};

static inline stplan*
st_plan(char *name)
{
	stplan *plan = malloc(sizeof(*plan));
	assert( plan != NULL );
	plan->name = name;
	plan->id = 0;
	plan->scene_count = 0;
	plan->group_count = 0;
	ss_listinit(&plan->group);
	ss_listinit(&plan->link);
	return plan;
}

static inline void
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
st_planreset(stplan *p)
{
	int i = 0;
	while (i < p->scene_count) {
		p->scene[i].state = 0;
		i++;
	}
}

static inline int
st_plannext(stplan *plan)
{
	int i = plan->scene_count - 1;
	while (i >= 0) {
		stscene *scene = &plan->scene[i];
		scene->state++;
		if (scene->state >= scene->statemax)
			scene->state = 0;
		else
			return 1;
		i--;
	}
	return 0;
}

static inline void
st_planadd(stplan *p, stgroup *g)
{
	ss_listappend(&p->group, &g->link);
	g->id = p->group_count++;
}

static inline void
st_planadd_scene(stplan *p, stscene *s)
{
	assert( s != NULL );
	int id = p->scene_count;
	p->scene[id] = *s;
	p->scene_count++;
}

#endif
