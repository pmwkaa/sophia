#ifndef ST_SUITE_H_
#define ST_SUITE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stsuite stsuite;

struct stsuite {
	sslist scene;
	int scene_count;
	sslist plan;
	int plan_count;
};

stscene *st_suitescene_of(stsuite*, char*);
void st_suiteinit(stsuite*);
void st_suitefree(stsuite*);
void st_suiterun(stsuite*);
void st_suiteadd(stsuite*, stplan*);
void st_suiteadd_scene(stsuite*, stscene*);

#endif
