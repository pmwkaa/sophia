#ifndef ST_H_
#define ST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stscene stscene;
typedef struct stsuite stsuite;
typedef struct st st;
typedef struct stc stc;

typedef void (*stscenef)(stscene*, stc*);
typedef void (*stf)(stc*);

struct stc {
	void *db;
	void *env;
	st *group;
	st *test;
	stf commit;
	int phase_scene;
	int phase;
	stsuite *suite;
};

struct st {
	char *name;
	stf function;
	srlist list;
	srlist link;
};

struct stscene {
	int statemax;
	int state;
	stscenef function;
};

struct stsuite {
	char *logdir;
	char *dir;
	srlist groups;
	srlist units;
	stscene scene[20];
	int scenecount;
};

static inline st*
st_def(char *name, stf function)
{
	st *test = malloc(sizeof(*test));
	t( test != NULL );
	test->name = name;
	test->function = function;
	sr_listinit(&test->list);
	sr_listinit(&test->link);
	return test;
}

static inline void
st_init(stsuite *s, char *dir, char *logdir)
{
	sr_listinit(&s->groups);
	sr_listinit(&s->units);
	s->dir = dir;
	s->logdir = logdir;
	memset(&s->scene, 0, sizeof(s->scene));
	s->scenecount = 0;
}

static inline void
st_free(stsuite *s)
{
	srlist *i, *n;
	sr_listforeach_safe(&s->groups, i, n) {
		st *group = srcast(i, st, link);
		srlist *j, *p;
		sr_listforeach_safe(&group->list, j, p) {
			st *test = srcast(j, st, link);
			free(test);
		}
		free(group);
	}
	sr_listforeach_safe(&s->units, i, n) {
		st *group = srcast(i, st, link);
		srlist *j, *p;
		sr_listforeach_safe(&group->list, j, p) {
			st *test = srcast(j, st, link);
			free(test);
		}
		free(group);
	}
}

static inline void
st_group(stsuite *s, st *group) {
	sr_listappend(&s->groups, &group->link);
}

static inline void
st_test(st *group, st *test) {
	sr_listappend(&group->list, &test->link);
}

static inline void
st_scene(stsuite *s, stscenef function, int statemax)
{
	int id = s->scenecount;
	s->scene[id].function = function;
	s->scene[id].state = 0;
	s->scene[id].statemax = statemax;
	s->scenecount++;
}

static inline void
st_unit(stsuite *s, st *test) {
	sr_listappend(&s->units, &test->link);
}

static inline void
st_transaction(stc *cx) {
	if (cx->commit)
		cx->commit(cx);
}

static inline int
st_next(stsuite *s)
{
	int i = s->scenecount - 1;
	while (i >= 0) {
		stscene *g = &s->scene[i];
		g->state++;
		if (g->state == g->statemax)
			g->state = 0;
		else
			return 1;
		i--;
	}
	return 0;
}

static inline void
st_rungroup(stsuite *s, st *group)
{
	srlist *i;
	sr_listforeach(&group->list, i)
	{
		st *test = srcast(i, st, link);
		stc context = {
			.env    = NULL,
			.db     = NULL,
			.group  = group,
			.test   = test,
			.suite  = s,
			.commit = NULL,
			.phase_scene = 0,
			.phase  = 0
		};
		printf("%s.%s", group->name, test->name);
		fflush(NULL);
		int i = 0;
		while (i < s->scenecount) {
			stscene *g = &s->scene[i];
			g->function(g, &context);
			i++;
		}
	}
}

static inline void
st_rununit(stsuite *s)
{
	srlist *i;
	sr_listforeach(&s->units, i) {
		st *group = srcast(i, st, link);
		srlist *j;
		sr_listforeach(&group->list, j) {
			st *test = srcast(j, st, link);
			printf("%s.%s", group->name, test->name);
			fflush(NULL);
			stc context = {
				.env    = NULL,
				.db     = NULL,
				.group  = group,
				.test   = test,
				.suite  = s,
				.commit = NULL,
				.phase_scene = 0,
				.phase  = 0
			};
			test->function(&context);
			printf(": ok\n");
			fflush(NULL);
		}
	}
}

static inline void
st_run(stsuite *s)
{
	srlist *i;
	sr_listforeach(&s->groups, i) {
		st *group = srcast(i, st, link);
		st_rungroup(s, group);
	}
}

int rmrf(char*);

#endif
