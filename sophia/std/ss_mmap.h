#ifndef SS_MMAP_H_
#define SS_MMAP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssmmap ssmmap;

struct ssmmap {
	char *p;
	size_t size;
};

static inline void
ss_mmapinit(ssmmap *m) {
	m->p = NULL;
	m->size = 0;
}

int ss_mmap(ssmmap*, int, uint64_t, int);
int ss_mmap_allocate(ssmmap*, uint64_t);
int ss_munmap(ssmmap*);

#endif
