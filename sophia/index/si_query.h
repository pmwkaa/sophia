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
	void *prefix;
	void *key;
	uint32_t keysize;
	uint32_t prefixsize;
	uint64_t vlsn;
	svmerge merge;
	sv result;
	sicache *cache;
	sr *r;
	si *index;
};

int si_queryopen(siquery*, sr*, sicache*, si*, srorder, uint64_t,
                 void*, uint32_t,
                 void*, uint32_t);
int si_queryclose(siquery*);
int si_querydup(siquery*, sv*);
int si_query(siquery*);
int si_querycommited(si*, sr*, sv*);

#endif
