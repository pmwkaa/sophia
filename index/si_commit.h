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
	uint64_t time;
	uint64_t vlsn;
	sv *v;
	si *index;
	sr *r;
};

void si_begin(sitx*, sr*, si*, uint64_t, uint64_t, sv*);
void si_commit(sitx*);
void si_set(si*, sr*, uint64_t, uint64_t, svv*);
void si_write(sitx*, int);

#endif
