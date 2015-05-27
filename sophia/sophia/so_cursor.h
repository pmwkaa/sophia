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
	srobj o;
	int async;
	int ready;
	ssorder order;
	sx t;
	sicache *cache;
	sv seek;
	void *prefix;
	int prefixsize;
	sov v;
	sodb *db;
} sspacked;

srobj *so_cursornew(sodb*, uint64_t, int, va_list);
void   so_cursorend(socursor*);

#endif
