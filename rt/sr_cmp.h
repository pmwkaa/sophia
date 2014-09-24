#ifndef SR_CMP_H_
#define SR_CMP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef int (*srcmpf)(char *a, size_t asz, char *b, size_t bsz, void *arg);

typedef struct srcomparator srcomparator;

struct srcomparator {
	srcmpf cmp;
	void *cmparg;
};

static inline int
sr_compare(srcomparator *c, char *a, size_t asize, char *b, size_t bsize)
{
	return c->cmp(a, asize, b, bsize, c->cmparg);
}

int sr_cmpu32(char*, size_t, char*, size_t, void*);
int sr_cmpstring(char*, size_t, char*, size_t, void*);

#endif
