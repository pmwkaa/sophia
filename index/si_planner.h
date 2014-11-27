#ifndef SI_PLANNER_H_
#define SI_PLANNER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siplanner siplanner;
typedef struct siplan siplan;

struct siplanner {
	srrb branch;
	srrb compact;
};

/* plan */
#define SI_BRANCH        1
#define SI_COMPACT       2
#define SI_COMPACT_INDEX 3

/* condition */
#define SI_CLSN          1

struct siplan {
	int plan;
	/* branch:
	 * compact_index:
	 *   a: index_size
	 *   b: ttl
	 *   c: lsn
	 * compact:
	 *   a: height
	 *   b:
	 *   c:
	 */
	int condition;
	uint64_t a; /* index size, branches */
	uint64_t b; /* ttl */
	uint64_t c; /* lsn */
	sinode *node;
};

int si_plannerinit(siplanner*);
int si_plannertrace(siplan*, srtrace*);
int si_plannerupdate(siplanner*, int, sinode*);
int si_plannerremove(siplanner*, int, sinode*);
sinode *si_planner(siplanner*, siplan*);

#endif
