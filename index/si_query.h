#ifndef SI_QUERY_H_
#define SI_QUERY_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siquery siquery;

struct siquery {
	srorder order;
	void *key;
	uint32_t keysize;
	uint64_t lsvn;
	sriter *firstsrc;
	svmerge merge;
	sv result;
	sr *r;
	si *index;
};

int si_queryopen(siquery*, sr*, si*, srorder, uint64_t,
                 void*, uint32_t);
int si_queryclose(siquery*);
int si_querydup(siquery*, sv*);
int si_queryfirstsrc(siquery*, sriter*);
int si_query(siquery*);
int si_querycommited(si*, sr*, sv*);

#endif
