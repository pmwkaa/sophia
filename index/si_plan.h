#ifndef SI_PLAN_H_
#define SI_PLAN_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siplan siplan;

struct siplan {
	srrb branch;
	srrb merge;
};

int si_planinit(siplan*);
int si_plan(siplan*, int, sinode*);
int si_planremove(siplan*, int, sinode*);
sinode *si_planpeek(siplan*, int, uint32_t);

void
si_planprint_branch(siplan*);
void
si_planprint_merge(siplan*);

#endif
