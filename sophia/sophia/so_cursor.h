#ifndef SO_CURSOR_H_
#define SO_CURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct socursor socursor;

struct socursor {
	soobj o;
	int async;
	int ready;
	srorder order;
	sx t;
	sicache *cache;
	sv seek;
	void *prefix;
	int prefixsize;
	sov v;
	sodb *db;
} srpacked;

soobj *so_cursornew(sodb*, uint64_t, int, va_list);
void   so_cursorend(socursor*);

#endif
