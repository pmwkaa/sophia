#ifndef SS_BUF_H_
#define SS_BUF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssbuf ssbuf;

struct ssbuf {
	char *reserve;
	char *s, *p, *e;
};

static inline void
ss_bufinit(ssbuf *b)
{
	b->reserve = NULL;
	b->s = NULL;
	b->p = NULL;
	b->e = NULL;
}

static inline void
ss_bufinit_reserve(ssbuf *b, void *buf, int size)
{
	b->reserve = buf;
	b->s = buf;
	b->p = b->s; 
	b->e = b->s + size;
}

static inline void
ss_buffree(ssbuf *b, ssa *a)
{
	if (ssunlikely(b->s == NULL))
		return;
	if (ssunlikely(b->s != b->reserve))
		ss_free(a, b->s);
	b->s = NULL;
	b->p = NULL;
	b->e = NULL;
}

static inline int
ss_bufsize(ssbuf *b) {
	return b->e - b->s;
}

static inline int
ss_bufused(ssbuf *b) {
	return b->p - b->s;
}

static inline int
ss_bufunused(ssbuf *b) {
	return b->e - b->p;
}

static inline void
ss_bufreset(ssbuf *b) {
	b->p = b->s;
}

static inline void
ss_bufgc(ssbuf *b, ssa *a, int wm)
{
	if (ssunlikely(ss_bufsize(b) >= wm)) {
		ss_buffree(b, a);
		ss_bufinit(b);
		return;
	}
	ss_bufreset(b);
}

static inline int
ss_bufensure(ssbuf *b, ssa *a, int size)
{
	if (sslikely(b->e - b->p >= size))
		return 0;
	int sz = ss_bufsize(b) * 2;
	int actual = ss_bufused(b) + size;
	if (ssunlikely(actual > sz))
		sz = actual;
	char *p;
	if (ssunlikely(b->s == b->reserve)) {
		p = ss_malloc(a, sz);
		if (ssunlikely(p == NULL))
			return -1;
		memcpy(p, b->s, ss_bufused(b));
	} else {
		p = ss_realloc(a, b->s, sz);
		if (ssunlikely(p == NULL))
			return -1;
	}
	b->p = p + (b->p - b->s);
	b->e = p + sz;
	b->s = p;
	assert((b->e - b->p) >= size);
	return 0;
}

static inline int
ss_buftruncate(ssbuf *b, ssa *a, int size)
{
	assert(size <= (b->p - b->s));
	char *p = b->reserve;
	if (b->s != b->reserve) {
		p = ss_realloc(a, b->s, size);
		if (ssunlikely(p == NULL))
			return -1;
	}
	b->p = p + (b->p - b->s);
	b->e = p + size;
	b->s = p;
	return 0;
}

static inline void
ss_bufadvance(ssbuf *b, int size)
{
	b->p += size;
}

static inline int
ss_bufadd(ssbuf *b, ssa *a, void *buf, int size)
{
	int rc = ss_bufensure(b, a, size);
	if (ssunlikely(rc == -1))
		return -1;
	memcpy(b->p, buf, size);
	ss_bufadvance(b, size);
	return 0;
}

static inline int
ss_bufin(ssbuf *b, void *v) {
	assert(b->s != NULL);
	return (char*)v >= b->s && (char*)v < b->p;
}

static inline void*
ss_bufat(ssbuf *b, int size, int i) {
	return b->s + size * i;
}

static inline void
ss_bufset(ssbuf *b, int size, int i, char *buf, int bufsize)
{
	assert(b->s + (size * i + bufsize) <= b->p);
	memcpy(b->s + size * i, buf, bufsize);
}

#endif
