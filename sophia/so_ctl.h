#ifndef SO_CTL_H_
#define SO_CTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soctl soctl;

struct soctl {
	soobj o;
	uint64_t memory_limit;
	uint32_t node_size;
	uint32_t node_page_size;
	uint32_t node_branch_wm;
	uint32_t node_merge_wm;
	uint32_t threads;
	void *e;
};

void  so_ctlinit(soctl*, void*);
int   so_ctldump(soctl*, srbuf*);
void *so_ctlreturn(srctl*, void*);

#endif
