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
	sslist nodelist;
	svlog *l;
	svlogindex *li;
	si *index;
};

void si_begin(sitx*, si*, uint64_t, uint64_t,
              svlog*, svlogindex*);
void si_commit(sitx*);
void si_write(sitx*, int);

#endif
