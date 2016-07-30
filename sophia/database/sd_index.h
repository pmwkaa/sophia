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
	uint16_t  align;
	uint32_t  extension;
	uint8_t   extensions;
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
	ssbuf i;
	sdindexheader *h;
};

static inline char*
sd_indexpage_min(sdindex *i, sdindexpage *p) {
	return (char*)i->i.s + p->offsetindex;
}

static inline char*
sd_indexpage_max(sdindex *i, sdindexpage *p) {
	return sd_indexpage_min(i, p) + p->sizemin;
}

static inline void
sd_indexinit(sdindex *i) {
	ss_bufinit(&i->i);
	i->h = NULL;
}

static inline void
sd_indexfree(sdindex *i, sr *r) {
	ss_buffree(&i->i, r->a);
}

static inline sdindexheader*
sd_indexheader(sdindex *i) {
	assert(i->i.s != NULL);
	return (sdindexheader*)(i->i.p - sizeof(sdindexheader));
}

static inline sdindexpage*
sd_indexpage(sdindex *i, uint32_t pos)
{
	assert(pos < i->h->count);
	sdindexpage *index =
		(sdindexpage*)
			((char*)i->h - (i->h->align +
			                i->h->extension +
			               (i->h->count * sizeof(sdindexpage))));
	return &index[pos];
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
	assert(i->h != NULL);
	return sd_indexheader(i)->keys;
}

static inline uint32_t
sd_indextotal(sdindex *i)
{
	assert(i->h != NULL);
	return sd_indexheader(i)->total;
}

static inline uint32_t
sd_indexsize_ext(sdindexheader *h)
{
	return h->align + h->size + h->extension + sizeof(sdindexheader);
}

static inline sdindexamqf*
sd_indexamqf(sdindex *i) {
	sdindexheader *h = sd_indexheader(i);
	assert(h->extensions & SD_INDEXEXT_AMQF);
	return (sdindexamqf*)(i->i.s + h->size);
}

static inline int
sd_indexcopy(sdindex *i, sr *r, sdindexheader *h)
{
	int size = sd_indexsize_ext(h);
	int rc = ss_bufensure(&i->i, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	char *start = (char*)h - (h->align + h->size + h->extension);
	memcpy(i->i.s, start, size);
	ss_bufadvance(&i->i, size);
	i->h = sd_indexheader(i);
	return 0;
}

static inline int
sd_indexcopy_buf(sdindex *i, sr *r, ssbuf *v, ssbuf *m)
{
	sdindexheader *h = (sdindexheader*)(m->p - sizeof(sdindexheader));
	int size = sd_indexsize_ext(h);
	assert(size == (ss_bufused(v) + ss_bufused(m)));
	int rc = ss_bufensure(&i->i, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	memcpy(i->i.s, v->s, ss_bufused(v));
	ss_bufadvance(&i->i, ss_bufused(v));
	memcpy(i->i.p, m->s, ss_bufused(m));
	ss_bufadvance(&i->i, ss_bufused(m));
	i->h = sd_indexheader(i);
	return 0;
}

#endif
