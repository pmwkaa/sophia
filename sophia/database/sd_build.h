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
	uint32_t c, csize;
} sspacked;

struct sdbuild {
	ssbuf       list;
	ssbuf       m, v, c;
	ssfilterif *compress_if;
	int         compress;
	int         crc;
	uint32_t    vmax;
	uint32_t    n;
};

void sd_buildinit(sdbuild*);
void sd_buildfree(sdbuild*, sr*);
void sd_buildreset(sdbuild*);
void sd_buildgc(sdbuild*, sr*, int);

static inline sdbuildref*
sd_buildref(sdbuild *b) {
	return ss_bufat(&b->list, sizeof(sdbuildref), b->n);
}

static inline sdpageheader*
sd_buildheader(sdbuild *b) {
	return (sdpageheader*)(b->m.s + sd_buildref(b)->m);
}

static inline char*
sd_buildmin(sdbuild *b, sr *r ssunused)
{
	sdbuildref *ref = sd_buildref(b);
	return b->v.s + ref->v;
}

static inline char*
sd_buildmax(sdbuild *b, sr *r)
{
	sdpageheader *h = sd_buildheader(b);
	sdbuildref *ref = sd_buildref(b);
	if (sf_schemefixed(r->scheme))
		return b->v.s + ref->v + (r->scheme->var_offset * (h->count - 1));
	return b->v.s + ref->v +
	       *(uint32_t*)((char*)h + sizeof(sdpageheader) +
	                    (sizeof(uint32_t) * (h->count - 1)));
}

int sd_buildbegin(sdbuild*, sr*, int, int, ssfilterif*);
int sd_buildend(sdbuild*, sr*);
int sd_buildcommit(sdbuild*);
int sd_buildadd(sdbuild*, sr*, char*, uint8_t);

#endif
