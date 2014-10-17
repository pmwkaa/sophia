#ifndef SI_COMMIT_H_
#define SI_COMMIT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sitx sitx;

struct sitx {
	uint64_t lsvn;
	svlog *log;
	svv *v;
	si *index;
	sr *r;
};

void si_begin(sitx*, sr*, si*, uint64_t, svlog*, svv*);
void si_commit(sitx*);
void si_rollback(sitx*);
void si_writelog_check(sitx*);
void si_writelog(sitx*);
void si_write(sitx*);

#endif
