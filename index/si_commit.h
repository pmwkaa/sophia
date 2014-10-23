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
	si *index;
	sr *r;
};

void si_begin(sitx*, sr*, si*, uint64_t, svlog*);
void si_commit(sitx*);
void si_rollback(sitx*);
void si_write(sitx*, int);

#endif
