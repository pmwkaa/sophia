#ifndef SE_CURSOR_H_
#define SE_CURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct secursor secursor;

struct secursor {
	so       o;
	int      async;
	int      ready;
	ssorder  order;
	sx       t;
	sv       seek;
	sv       v;
	void    *prefix;
	int      prefixsize;
	sicache *cache;
	sedb    *db;
};

so   *se_cursornew(sedb*, sev*, uint64_t, int);
void  se_cursorend(secursor*);

#endif
