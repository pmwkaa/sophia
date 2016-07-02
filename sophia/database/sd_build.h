#ifndef SD_BUILD_H_
#define SD_BUILD_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdbuild sdbuild;

struct sdbuild {
	ssbuf       m, v, c;
	ssfilterif *compress_if;
	int         compress;
	int         crc;
	uint32_t    vmax;
};

void sd_buildinit(sdbuild*);
void sd_buildfree(sdbuild*, sr*);
void sd_buildreset(sdbuild*);
void sd_buildgc(sdbuild*, sr*, int);

static inline sdpageheader*
sd_buildheader(sdbuild *b) {
	return (sdpageheader*)(b->m.s);
}

static inline char*
sd_buildmin(sdbuild *b, sr *r ssunused)
{
	return b->v.s;
}

static inline char*
sd_buildmax(sdbuild *b, sr *r)
{
	sdpageheader *h = sd_buildheader(b);
	if (sf_schemefixed(r->scheme))
		return b->v.s + (r->scheme->var_offset * (h->count - 1));
	return b->v.s +
	       *(uint32_t*)((char*)h + sizeof(sdpageheader) +
	                    (sizeof(uint32_t) * (h->count - 1)));
}

int sd_buildbegin(sdbuild*, sr*, int, int, ssfilterif*);
int sd_buildend(sdbuild*, sr*);
int sd_buildadd(sdbuild*, sr*, char*, uint8_t);

#endif
