#ifndef SY_H_
#define SY_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sy sy;

struct sy {
	syconf *conf;
};

int sy_init(sy*);
int sy_open(sy*, sr*, syconf*);
int sy_close(sy*, sr*);

#endif
