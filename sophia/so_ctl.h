#ifndef SO_CTL_H_
#define SO_CTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soctl soctl;

struct soctl {
	soobj o;
	void *e;
};

void  so_ctlinit(soctl*, void*);
int   so_ctldump(soctl*, srbuf*);

void *so_ctlreturn(srctl *match, void*);

#endif