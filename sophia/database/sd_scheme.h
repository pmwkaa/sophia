#ifndef SD_SCHEME_H_
#define SD_SCHEME_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdschemeheader sdschemeheader;
typedef struct sdschemeopt sdschemeopt;
typedef struct sdscheme sdscheme;

struct sdschemeheader {
	uint32_t crc;
	uint32_t size;
	uint32_t count;
} srpacked;

struct sdschemeopt {
	uint8_t  type;
	uint8_t  id;
	uint32_t size;
} srpacked;

struct sdscheme {
	srbuf buf;
};

static inline void
sd_schemeinit(sdscheme *c) {
	sr_bufinit(&c->buf);
}

static inline void
sd_schemefree(sdscheme *c, sr *r) {
	sr_buffree(&c->buf, r->a);
}

static inline char*
sd_schemesz(sdschemeopt *o) {
	assert(o->type == SR_STRING);
	return (char*)o + sizeof(sdschemeopt);
}

static inline uint32_t
sd_schemeu32(sdschemeopt *o) {
	assert(o->type == SR_U32);
	return *(uint32_t*)((char*)o + sizeof(sdschemeopt));
}

static inline uint64_t
sd_schemeu64(sdschemeopt *o) {
	assert(o->type == SR_U64);
	return *(uint64_t*)((char*)o + sizeof(sdschemeopt));
}

int sd_schemebegin(sdscheme*, sr*);
int sd_schemeadd(sdscheme*, sr*, uint8_t, srtype, void*, uint32_t);
int sd_schemecommit(sdscheme*, sr*);
int sd_schemewrite(sdscheme*, sr*, char*, int);
int sd_schemerecover(sdscheme*, sr*, char*);

#endif
