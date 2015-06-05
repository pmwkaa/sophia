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
	ssbuf dump;
	int first;
	srcv *pos;
} sspacked;

srobj *so_ctlcursor_new(void*);

#endif
