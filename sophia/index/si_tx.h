#ifndef SI_TX_H_
#define SI_TX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sitx sitx;

struct sitx {
	int ro;
	sslist nodelist;
	si *index;
};

void si_begin(sitx*, si*, int);
void si_commit(sitx*);

static inline void
si_txtrack(sitx *x, sinode *n) {
	if (ss_listempty(&n->commit))
		ss_listappend(&x->nodelist, &n->commit);
}

#endif
