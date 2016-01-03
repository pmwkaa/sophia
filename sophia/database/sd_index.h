#ifndef SD_INDEX_H_
#define SD_INDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdindexheader sdindexheader;
typedef struct sdindexamqf sdindexamqf;
typedef struct sdindexpage sdindexpage;
typedef struct sdindex sdindex;

#define SD_INDEXEXT_AMQF 1

struct sdindexheader {
	uint32_t  crc;
	srversion version;
	sdid      id;
	uint64_t  offset;
	uint32_t  size;
	uint32_t  sizevmax;
	uint32_t  count;
	uint32_t  keys;
	uint64_t  total;
	uint64_t  totalorigin;
	uint32_t  tsmin;
	uint64_t  lsnmin;
	uint64_t  lsnmax;
	uint32_t  dupkeys;
	uint64_t  dupmin;
	uint32_t  extension;
	uint8_t   extensions;
	char      reserve[31];
} sspacked;

struct sdindexamqf {
	uint8_t  q, r;
	uint32_t entries;
	uint32_t size;
	uint64_t table[];
} sspacked;

struct sdindexpage {
	uint64_t offset;
	uint32_t offsetindex;
	uint32_t size;
	uint32_t sizeorigin;
	uint16_t sizemin;
	uint16_t sizemax;
	uint64_t lsnmin;
	uint64_t lsnmax;
} sspacked;

struct sdindex {
	ssbuf i, v;
	sdindexheader *h;
};

static inline char*
sd_indexpage_min(sdindex *i, sdindexpage *p) {
	return (char*)i->i.s + sizeof(sdindexheader) +
	             (i->h->count * sizeof(sdindexpage)) + p->offsetindex;
}

static inline char*
sd_indexpage_max(sdindex *i, sdindexpage *p) {
	return sd_indexpage_min(i, p) + p->sizemin;
}

static inline void
sd_indexinit(sdindex *i) {
	ss_bufinit(&i->i);
	ss_bufinit(&i->v);
	i->h = NULL;
}

static inline void
sd_indexfree(sdindex *i, sr *r) {
	ss_buffree(&i->i, r->a);
	ss_buffree(&i->v, r->a);
}

static inline sdindexheader*
sd_indexheader(sdindex *i) {
	return (sdindexheader*)(i->i.s);
}

static inline sdindexpage*
sd_indexpage(sdindex *i, uint32_t pos)
{
	assert(pos < i->h->count);
	char *p = (char*)ss_bufat(&i->i, sizeof(sdindexpage), pos);
   	p += sizeof(sdindexheader);
	return (sdindexpage*)p;
}

static inline sdindexpage*
sd_indexmin(sdindex *i) {
	return sd_indexpage(i, 0);
}

static inline sdindexpage*
sd_indexmax(sdindex *i) {
	return sd_indexpage(i, i->h->count - 1);
}

static inline uint32_t
sd_indexkeys(sdindex *i)
{
	if (ssunlikely(i->i.s == NULL))
		return 0;
	return sd_indexheader(i)->keys;
}

static inline uint32_t
sd_indextotal(sdindex *i)
{
	if (ssunlikely(i->i.s == NULL))
		return 0;
	return sd_indexheader(i)->total;
}

static inline uint32_t
sd_indexsize_ext(sdindexheader *h)
{
	return sizeof(sdindexheader) + h->size + h->extension;
}

static inline sdindexamqf*
sd_indexamqf(sdindex *i) {
	sdindexheader *h = sd_indexheader(i);
	assert(h->extensions & SD_INDEXEXT_AMQF);
	return (sdindexamqf*)(i->i.s + sizeof(sdindexheader) + h->size);
}

int sd_indexbegin(sdindex*, sr*);
int sd_indexcommit(sdindex*, sr*, sdid*, ssqf*, uint64_t);
int sd_indexadd(sdindex*, sr*, sdbuild*, uint64_t);
int sd_indexcopy(sdindex*, sr*, sdindexheader*);

#endif
