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
	uint32_t k, ksize;
	uint32_t v, vsize;
	uint32_t c, csize;
} srpacked;

struct sdbuild {
	srbuf list, k, v, c;
	int compress;
	int crc;
	uint32_t n;
};

static inline void
sd_buildinit(sdbuild *b)
{
	sr_bufinit(&b->list);
	sr_bufinit(&b->k);
	sr_bufinit(&b->v);
	sr_bufinit(&b->c);
	b->n = 0;
	b->compress = 0;
	b->crc = 0;
}

static inline void
sd_buildfree(sdbuild *b, sr *r)
{
	sr_buffree(&b->list, r->a);
	sr_buffree(&b->k, r->a);
	sr_buffree(&b->v, r->a);
	sr_buffree(&b->c, r->a);
}

static inline void
sd_buildreset(sdbuild *b)
{
	sr_bufreset(&b->list);
	sr_bufreset(&b->k);
	sr_bufreset(&b->v);
	sr_bufreset(&b->c);
	b->n = 0;
}

static inline sdbuildref*
sd_buildref(sdbuild *b) {
	return sr_bufat(&b->list, sizeof(sdbuildref), b->n);
}

static inline sdpageheader*
sd_buildheader(sdbuild *b) {
	return (sdpageheader*)(b->k.s + sd_buildref(b)->k);
}

static inline uint64_t
sd_buildoffset(sdbuild *b)
{
	sdbuildref *r = sd_buildref(b);
	if (b->compress)
		return r->c;
	return r->k + sr_bufused(&b->v) - (sr_bufused(&b->v) - r->v);
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

int sd_buildbegin(sdbuild*, sr*, int, int);
int sd_buildend(sdbuild*, sr*);
int sd_buildcommit(sdbuild*);
int sd_buildadd(sdbuild*, sr*, sv*, uint32_t);

#endif
