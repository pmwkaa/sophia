#ifndef SS_MAP_H_
#define SS_MAP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssmap ssmap;

struct ssmap {
	char *p;
	size_t size;
};

static inline void
ss_mapinit(ssmap *m) {
	m->p = NULL;
	m->size = 0;
}

int ss_map(ssmap*, int, uint64_t, int);
int ss_mapunmap(ssmap*);

static inline int
ss_mapfile(ssmap *map, ssfile *f, int ro)
{
	return ss_map(map, f->fd, f->size, ro);
}

#endif
