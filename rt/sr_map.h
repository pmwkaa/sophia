#ifndef SR_MAP_H_
#define SR_MAP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srmap srmap;

struct srmap {
	char *p;
	size_t size;
};

static inline void
sr_mapinit(srmap *m) {
	m->p = NULL;
	m->size = 0;
}

int sr_map(srmap*, int, uint64_t, int);
int sr_mapunmap(srmap*);

static inline int
sr_mapfile(srmap *map, srfile *f, int ro)
{
	return sr_map(map, f->fd, f->size, ro);
}

#endif
