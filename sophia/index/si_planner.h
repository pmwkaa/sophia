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

typedef enum {
	SI_PNONE,
	SI_PMATCH,
	SI_PRETRY
} siplannerrc;

struct siplanner {
	ssrq  memory;
	void *i;
};

/* plan */
#define SI_COMPACT_INDEX 1
#define SI_EXPIRE        2
#define SI_GC            4
#define SI_NODEGC        8
#define SI_BACKUP        16
#define SI_BACKUPEND     32

struct siplan {
	int plan;
	/* compact_index:
	 * gc:
	 *   a: lsn
	 *   b: percent
	 * expire:
	 *   a: ttl
	 * nodegc:
	 * backup:
	 *   a: bsn
	 */
	uint64_t a, b, c;
	sinode *node;
};

int si_planinit(siplan*);
int si_plannerinit(siplanner*, ssa*, void*);
int si_plannerfree(siplanner*, ssa*);
int si_plannertrace(siplan*, uint32_t, sstrace*);
int si_plannerupdate(siplanner*, sinode*);
int si_plannerremove(siplanner*, sinode*);
siplannerrc
si_planner(siplanner*, siplan*);

#endif
