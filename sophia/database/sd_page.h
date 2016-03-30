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

static inline uint64_t
sd_pagesizeof(sdpage *p, sdv *v)
{
	char *ptr = sd_pagepointer(p, v);
	uint64_t val = 0;
	ss_leb128read(ptr, &val);
	return val;
}

static inline uint64_t
sd_pagelsnof(sdpage *p, sdv *v)
{
	char *ptr = sd_pagepointer(p, v);
	ptr += ss_leb128skip(ptr);
	uint64_t val;
	ss_leb128read(ptr, &val);
	return val;
}

static inline uint32_t
sd_pagetimestampof(sdpage *p, sdv *v)
{
	if (! sv_isflags(v->flags, SVTIMESTAMP))
		return UINT32_MAX;
	char *ptr = sd_pagepointer(p, v);
	ptr += ss_leb128skip(ptr);
	ptr += ss_leb128skip(ptr);
	uint64_t ts;
	ss_leb128read(ptr, &ts);
	return ts;
}

static inline char*
sd_pagemetaof(sdpage *p, sdv *v, uint64_t *size, uint64_t *lsn,
             uint64_t *timestamp)
{
	char *ptr = sd_pagepointer(p, v);
	ptr += ss_leb128read(ptr, size);
	ptr += ss_leb128read(ptr, lsn);
	if (! sv_isflags(v->flags, SVTIMESTAMP)) {
		*timestamp = UINT32_MAX;
		return ptr;
	}
	ptr += ss_leb128read(ptr, timestamp);
	return ptr;
}

#endif
