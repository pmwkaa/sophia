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
	srcmpf prefix;
	void *prefixarg;
};

static inline int
sr_compare(srcomparator *c, char *a, size_t asize, char *b, size_t bsize)
{
	return c->cmp(a, asize, b, bsize, c->cmparg);
}

static inline int
sr_compareprefix(srcomparator *c, char *prefix, size_t prefixsize,
                 char *key, size_t keysize)
{
	return c->prefix(prefix, prefixsize, key, keysize, c->prefixarg);
}

int sr_cmpu32(char*, size_t, char*, size_t, void*);
int sr_cmpstring(char*, size_t, char*, size_t, void*);
int sr_cmpstring_prefix(char*, size_t, char*, size_t, void*);
int sr_cmpset(srcomparator*, char*);
int sr_cmpsetarg(srcomparator*, char*);
int sr_cmpset_prefix(srcomparator*, char*);
int sr_cmpset_prefixarg(srcomparator*, char*);

#endif
