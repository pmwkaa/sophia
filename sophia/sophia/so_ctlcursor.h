#ifndef SO_CTLCURSOR_H_
#define SO_CTLCURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soctlcursor soctlcursor;

struct soctlcursor {
	srobj o;
	int ready;
	ssbuf dump;
	srcv *pos;
	srobj *v;
} sspacked;

srobj *so_ctlcursor_new(void*);

#endif
