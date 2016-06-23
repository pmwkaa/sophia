#ifndef SD_PAGE_H_
#define SD_PAGE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdpageheader sdpageheader;
typedef struct sdpage sdpage;

struct sdpageheader {
	uint32_t crc;
	uint32_t crcdata;
	uint32_t count;
	uint32_t countdup;
	uint32_t sizeorigin;
	uint32_t size;
	uint64_t lsnmin;
	uint64_t lsnmindup;
	uint64_t lsnmax;
	uint32_t tsmin;
	uint32_t reserve;
} sspacked;

struct sdpage {
	sdpageheader *h;
};

static inline void
sd_pageinit(sdpage *p, sdpageheader *h) {
	p->h = h;
}

static inline char*
sd_pagepointer(sdpage *p, sr *r, uint32_t pos)
{
	assert(pos < p->h->count);
	char *ptr = (char*)p->h + sizeof(sdpageheader);
	if (sf_schemefixed(r->scheme))
		return ptr + (r->scheme->var_offset * pos);
	uint32_t *offset = (uint32_t*)ptr;
	assert((sizeof(uint32_t) * p->h->count) +
	        offset[pos] <= p->h->sizeorigin);
	return ptr + (sizeof(uint32_t) * p->h->count) + offset[pos];
}

#endif
