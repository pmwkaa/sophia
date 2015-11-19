#ifndef SS_BLOB_H_
#define SS_BLOB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssblob ssblob;

struct ssblob {
	ssmmap map;
	char *s, *p, *e;
	ssvfs *vfs;
};

static inline void
ss_blobinit(ssblob *b, ssvfs *vfs)
{
	ss_mmapinit(&b->map);
	b->s   = NULL;
	b->p   = NULL;
	b->e   = NULL;
	b->vfs = vfs;
}

static inline int
ss_blobfree(ssblob *b)
{
	return ss_vfsmunmap(b->vfs, &b->map);
}

static inline void
ss_blobreset(ssblob *b) {
	b->p = b->s;
}

static inline int
ss_blobsize(ssblob *b) {
	return b->e - b->s;
}

static inline int
ss_blobused(ssblob *b) {
	return b->p - b->s;
}

static inline int
ss_blobunused(ssblob *b) {
	return b->e - b->p;
}

static inline void
ss_blobadvance(ssblob *b, int size)
{
	b->p += size;
}

static inline int
ss_blobrealloc(ssblob *b, int size)
{
	int rc = ss_vfsmremap(b->vfs, &b->map, size);
	if (ssunlikely(rc == -1))
		return -1;
	char *p = b->map.p;
	b->p = p + (b->p - b->s);
	b->e = p + size;
	b->s = p;
	assert((b->e - b->p) <= size);
	return 0;
}

static inline int
ss_blobensure(ssblob *b, int size)
{
	if (sslikely(b->e - b->p >= size))
		return 0;
	int sz = ss_blobsize(b) * 2;
	int actual = ss_blobused(b) + size;
	if (ssunlikely(actual > sz))
		sz = actual;
	return ss_blobrealloc(b, sz);
}

static inline int
ss_blobfit(ssblob *b)
{
	if (ss_blobunused(b) == 0)
		return 0;
	return ss_blobrealloc(b, ss_blobused(b));
}

static inline int
ss_blobadd(ssblob *b, void *buf, int size)
{
	int rc = ss_blobensure(b, size);
	if (ssunlikely(rc == -1))
		return -1;
	memcpy(b->p, buf, size);
	ss_blobadvance(b, size);
	return 0;
}

#endif
