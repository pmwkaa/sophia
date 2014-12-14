#ifndef SO_LOGCURSOR_H_
#define SO_LOGCURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sologcursor sologcursor;

struct sologcursor {
	soobj o;
	int ready;
	svlogv *pos;
	sov v;
	sotx *t;
} srpacked;

soobj *so_logcursor_new(sotx*);

#endif
