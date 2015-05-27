#ifndef ST_H_
#define ST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sttest sttest;
typedef struct stgroup stgroup;
typedef struct stscene stscene;
typedef struct stplan stplan;
typedef struct stc stc;
typedef struct st st;

typedef void (*stscenef)(stscene*, stc*);
typedef void (*stf)(stc*);

struct stc {
	void *env, *db;
	int phase_scene;
	int phase;
	stf commit;
	sttest *test;
	stgroup *group;
	stplan *plan;
	st *suite;
};

struct sttest {
	char *name;
	stf function;
	sslist link;
};

struct stgroup {
	char *name;
	sslist test;
	int count;
	sslist link;
};

struct stscene {
	char *name;
	int statemax;
	int state;
	stscenef function;
	sslist link;
};

struct stplan {
	char *name;
	sslist group;
	stscene scene[20];
	int group_count;
	int scene_count;
	sslist link;
};

struct st {
	char *sophiadir;
	char *backupdir;
	char *logdir;
	char *dir;
	sslist scene;
	sslist plan;
	int scene_count;
	int plan_count;
	int stat_stmt;
	int stat_test;
};

void     st_init(st*, char*, char*, char*, char*);
void     st_free(st*);
void     st_run(st*);
stscene *st_sceneof(st*, char*);
void     st_addscene(st*, stscene*);
void     st_add(st*, stplan*);
stscene *st_scene(char*, stscenef, int);
stplan  *st_plan(char*);
void     st_planadd(stplan*, stgroup*);
void     st_planscene(stplan*, stscene*);
stgroup *st_group(char*);
void     st_groupadd(stgroup*, sttest*);
sttest  *st_test(char*, stf);
void     st_transaction(stc*);
void     st_error(stc*);

#endif
