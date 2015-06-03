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
	sv seek;
	sv v;
	void *prefix;
	int prefixsize;
	sicache *cache;
	sodb *db;
} sspacked;

srobj *so_cursornew(sodb*, uint64_t, int, va_list);
void   so_cursorend(socursor*);

#endif
