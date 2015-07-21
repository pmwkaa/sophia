#ifndef ST_GROUP_H_
#define ST_GROUP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stgroup stgroup;

struct stgroup {
	char *name;
	int id;
	sslist test;
	int count;
	sslist link;
};

static inline stgroup*
st_group(char *name)
{
	stgroup *group = malloc(sizeof(*group));
	assert( group != NULL );
	group->name = name;
	group->id = 0;
	group->count = 0;
	ss_listinit(&group->test);
	ss_listinit(&group->link);
	return group;
}

static inline void
st_groupfree(stgroup *group)
{
	sslist *i, *n;
	ss_listforeach_safe(&group->test, i, n) {
		sttest *test = sscast(i, sttest, link);
		st_testfree(test);
	}
	free(group);
}

static inline void
st_groupadd(stgroup *g, sttest *t)
{
	ss_listappend(&g->test, &t->link);
	t->id = g->count++;
}

#endif
