#ifndef SD_BUILD_H_
#define SD_BUILD_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdbuildref sdbuildref;
typedef struct sdbuild sdbuild;

struct sdbuildref {
	uint32_t m, msize;
	uint32_t v, vsize;
	uint32_t k, ksize;
	uint32_t c, csize;
} sspacked;

struct sdbuild {
	ssbuf list, m, v, k, c;
	ssfilterif *compress_if;
	int compress_dup;
	int compress;
	int crc;
	uint32_t vmax;
	uint32_t n;
	ssht tracker;
};

void sd_buildinit(sdbuild*);
void sd_buildfree(sdbuild*, sr*);
void sd_buildreset(sdbuild*, sr*);
void sd_buildgc(sdbuild*, sr*, int);

static inline sdbuildref*
sd_buildref(sdbuild *b) {
	return ss_bufat(&b->list, sizeof(sdbuildref), b->n);
}

static inline sdpageheader*
sd_buildheader(sdbuild *b) {
	return (sdpageheader*)(b->m.s + sd_buildref(b)->m);
}

static inline uint64_t
sd_buildoffset(sdbuild *b)
{
	sdbuildref *r = sd_buildref(b);
	if (b->compress)
		return r->c;
	return r->m + (ss_bufused(&b->v) - (ss_bufused(&b->v) - r->v)) +
	              (ss_bufused(&b->k) - (ss_bufused(&b->k) - r->k));
}

static inline sdv*
sd_buildmin(sdbuild *b) {
	return (sdv*)((char*)sd_buildheader(b) + sizeof(sdpageheader));
}

static inline char*
sd_buildminkey(sdbuild *b) {
	sdbuildref *r = sd_buildref(b);
	return b->v.s + r->v + sd_buildmin(b)->offset;
}

static inline sdv*
sd_buildmax(sdbuild *b) {
	sdpageheader *h = sd_buildheader(b);
	return (sdv*)((char*)h + sizeof(sdpageheader) + sizeof(sdv) * (h->count - 1));
}

static inline char*
sd_buildmaxkey(sdbuild *b) {
	sdbuildref *r = sd_buildref(b);
	return b->v.s + r->v + sd_buildmax(b)->offset;
}

int sd_buildbegin(sdbuild*, sr*, int, int, int, ssfilterif*);
int sd_buildend(sdbuild*, sr*);
int sd_buildcommit(sdbuild*, sr*);
int sd_buildadd(sdbuild*, sr*, sv*, uint32_t);

#endif
