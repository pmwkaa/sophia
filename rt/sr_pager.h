#ifndef SR_PAGER_H_
#define SR_PAGER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srpagepool srpagepool;
typedef struct srpage srpage;
typedef struct srpager srpager;

struct srpagepool {
	uint32_t used;
	srpagepool *next;
} srpacked;

struct srpage {
	srpagepool *pool;
	srpage *next;
} srpacked;

struct srpager {
	uint32_t page_size;
	uint32_t pool_count;
	uint32_t pool_size;
	uint32_t pools;
	srpagepool *pp;
	srpage *p;
};

void  sr_pagerinit(srpager*, uint32_t, uint32_t);
void  sr_pagerfree(srpager*);
int   sr_pageradd(srpager*);
void *sr_pagerpop(srpager*);
void  sr_pagerpush(srpager*, srpage*);

#endif
