#ifndef ST_H_
#define ST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stconf stconf;

struct stconf {
	char *sophia_dir;
	char *backup_dir;
	char *log_dir;
	char *db_dir;
	char *id;
	char *logfile;
	int   stop_plan;
	int   stop_group;
	int   stop_test;
	int   report;
	int   verbose;
};

void st_init(stconf*);
void st_free(void);
void st_phase(void);
void st_run(void);
void st_seedset(int);
int  st_seed(void);

#endif
