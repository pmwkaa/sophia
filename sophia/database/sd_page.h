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
	uint32_t sizekeys;
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

static inline sdv*
sd_pagev(sdpage *p, uint32_t pos) {
	assert(pos < p->h->count);
	return (sdv*)((char*)p->h + sizeof(sdpageheader) + sizeof(sdv) * pos);
}

static inline sdv*
sd_pagemin(sdpage *p) {
	return sd_pagev(p, 0);
}

static inline sdv*
sd_pagemax(sdpage *p) {
	return sd_pagev(p, p->h->count - 1);
}

static inline void*
sd_pagepointer(sdpage *p, sdv *v) {
	assert((sizeof(sdv) * p->h->count) + v->offset <= p->h->sizeorigin);
	return ((char*)p->h + sizeof(sdpageheader) +
	         sizeof(sdv) * p->h->count) + v->offset;
}

#endif
