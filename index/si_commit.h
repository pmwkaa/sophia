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

int si_begin(sitx*, sr*, si*, uint64_t, svlog*, svv*);
int si_commit(sitx*);
int si_rollback(sitx*);
int si_writelog(sitx*);
int si_write(sitx*);

#endif
