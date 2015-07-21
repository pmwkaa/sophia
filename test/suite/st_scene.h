#ifndef ST_SCENE_H_
#define ST_SCENE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stscene stscene;

typedef void (*stscenef)(stscene*);

struct stscene {
	char *name;
	int statemax;
	int state;
	stscenef function;
	sslist link;
};

static inline stscene*
st_scene(char *name, stscenef function, int statemax)
{
	assert(statemax < 11); /* single id char */
	stscene *scene = malloc(sizeof(*scene));
	if (ssunlikely(scene == NULL))
		return NULL;
	scene->name = name;
	scene->state = 0;
	scene->statemax = statemax;
	scene->function = function;
	ss_listinit(&scene->link);
	return scene;
}

static inline void
st_scenefree(stscene *scene)
{
	free(scene);
}

#endif
