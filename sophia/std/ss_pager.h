#ifndef SS_PAGER_H_
#define SS_PAGER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sspagepool sspagepool;
typedef struct sspage sspage;
typedef struct sspager sspager;

struct sspagepool {
	uint32_t used;
	sspagepool *next;
} sspacked;

struct sspage {
	sspagepool *pool;
	sspage *next;
} sspacked;

struct sspager {
	uint32_t page_size;
	uint32_t pool_count;
	uint32_t pool_size;
	uint32_t pools;
	sspagepool *pp;
	sspage *p;
	ssvfs *vfs;
};

void  ss_pagerinit(sspager*, ssvfs*, uint32_t, uint32_t);
void  ss_pagerfree(sspager*);
int   ss_pageradd(sspager*);
void *ss_pagerpop(sspager*);
void  ss_pagerpush(sspager*, sspage*);

#endif
