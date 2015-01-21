#ifndef SR_BUF_H_
#define SR_BUF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srbuf srbuf;

struct srbuf {
	char *reserve;
	char *s, *p, *e;
};

static inline void
sr_bufinit(srbuf *b)
{
	b->reserve = NULL;
	b->s = NULL;
	b->p = NULL;
	b->e = NULL;
}

static inline void
sr_bufinit_reserve(srbuf *b, void *buf, int size)
{
	b->reserve = buf;
	b->s = buf;
	b->p = b->s; 
	b->e = b->s + size;
}

static inline void
sr_buffree(srbuf *b, sra *a)
{
	if (srunlikely(b->s == NULL))
		return;
	if (srunlikely(b->s != b->reserve))
		sr_free(a, b->s);
	b->s = NULL;
	b->p = NULL;
	b->e = NULL;
}

static inline void
sr_bufreset(srbuf *b) {
	b->p = b->s;
}

static inline int
sr_bufsize(srbuf *b) {
	return b->e - b->s;
}

static inline int
sr_bufused(srbuf *b) {
	return b->p - b->s;
}

static inline int
sr_bufensure(srbuf *b, sra *a, int size)
{
	if (srlikely(b->e - b->p >= size))
		return 0;
	int sz = sr_bufsize(b) * 2;
	int actual = sr_bufused(b) + size;
	if (srunlikely(actual > sz))
		sz = actual;
	char *p;
	if (srunlikely(b->s == b->reserve)) {
		p = sr_malloc(a, sz);
		if (srunlikely(p == NULL))
			return -1;
		memcpy(p, b->s, sr_bufused(b));
	} else {
		p = sr_realloc(a, b->s, sz);
		if (srunlikely(p == NULL))
			return -1;
	}
	b->p = p + (b->p - b->s);
	b->e = p + sz;
	b->s = p;
	assert((b->e - b->p) >= size);
	return 0;
}

static inline int
sr_buftruncate(srbuf *b, sra *a, int size)
{
	assert(size <= (b->p - b->s));
	char *p = b->reserve;
	if (b->s != b->reserve) {
		p = sr_realloc(a, b->s, size);
		if (srunlikely(p == NULL))
			return -1;
	}
	b->p = p + (b->p - b->s);
	b->e = p + size;
	b->s = p;
	return 0;
}

static inline void
sr_bufadvance(srbuf *b, int size)
{
	b->p += size;
}

static inline int
sr_bufadd(srbuf *b, sra *a, void *buf, int size)
{
	int rc = sr_bufensure(b, a, size);
	if (srunlikely(rc == -1))
		return -1;
	memcpy(b->p, buf, size);
	sr_bufadvance(b, size);
	return 0;
}

static inline int
sr_bufin(srbuf *b, void *v) {
	assert(b->s != NULL);
	return (char*)v >= b->s && (char*)v < b->p;
}

static inline void*
sr_bufat(srbuf *b, int size, int i) {
	return b->s + size * i;
}

static inline void
sr_bufset(srbuf *b, int size, int i, char *buf, int bufsize)
{
	assert(b->s + (size * i + bufsize) <= b->p);
	memcpy(b->s + size * i, buf, bufsize);
}

#endif
