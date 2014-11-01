
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

int si_plannerinit(siplanner *p)
{
	sr_rbinit(&p->branch);
	sr_rbinit(&p->merge);
	return 0;
}

int si_plannertrace(siplan *plan, srtrace *t)
{
	if (plan->plan == SI_BRANCH) {
		sr_trace(t, "branch (node: %" PRIu32 ")", plan->node->id.id);
		return 0;
	}
	sr_trace(t, "merge (node: %" PRIu32 ")", plan->node->id.id);
	return 0;
}

srhot static inline int
si_plannermerge_cmp(sinode *a, sinode *b)
{
	if (a->lv != b->lv)
		return (a->lv > b->lv) ? 1 : -1;
	if (a->id.id == b->id.id)
		return 0;
	return (a->id.id > b->id.id) ? 1 : -1;
}

sr_rbget(si_plannermerge_match,
         si_plannermerge_cmp(srcast(n, sinode, nodemerge), (sinode*)key))

static inline int
si_plannermerge(siplanner *p, sinode *n)
{
	sr_rbremove(&p->merge, &n->nodemerge);
	srrbnode *pn = NULL;
	int rc = si_plannermerge_match(&p->merge, NULL, n, 0, &pn);
	assert(! (rc == 0 && pn));
	sr_rbset(&p->merge, pn, rc, &n->nodemerge);
#if 0
	pn = sr_rbmax(&p->merge);
	if (pn == NULL)
		return 0;
	n = srcast(pn, sinode, nodemerge);
	uint32_t lvlast = n->lv;
	pn = sr_rbprev(&p->merge, pn);
	while (pn) {
		n = srcast(pn, sinode, nodemerge);
		assert(n->lv <= lvlast);
		lvlast = n->lv;
		pn = sr_rbprev(&p->merge, pn);
	}
#endif
	return 0;
}

srhot static inline int
si_plannerbranch_cmp(sinode *a, sinode *b)
{
	if (a->iused != b->iused)
		return (a->iused > b->iused) ? 1 : -1;
	if (a->id.id == b->id.id)
		return 0;
	return (a->id.id > b->id.id) ? 1 : -1;
}

sr_rbget(si_plannerbranch_match,
         si_plannerbranch_cmp(srcast(n, sinode, nodebranch), (sinode*)key))

static inline int
si_plannerbranch(siplanner *p, sinode *n)
{
	sr_rbremove(&p->branch, &n->nodebranch);
	srrbnode *pn = NULL;
	int rc = si_plannerbranch_match(&p->branch, NULL, n, 0, &pn);
	assert(! (rc == 0 && pn));
	sr_rbset(&p->branch, pn, rc, &n->nodebranch);
#if 0
	pn = sr_rbmax(&p->branch);
	if (pn == NULL)
		return 0;
	n = srcast(pn, sinode, nodebranch);
	uint32_t iusedlast = n->iused;
	pn = sr_rbprev(&p->branch, pn);
	while (pn) {
		n = srcast(pn, sinode, nodebranch);
		assert(n->iused <= iusedlast);
		iusedlast = n->iused;
		pn = sr_rbprev(&p->branch, pn);
	}
#endif
	return 0;
}

int si_plannerupdate(siplanner *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		si_plannerbranch(p, n);
	if (mask & SI_MERGE)
		si_plannermerge(p, n);
	return 0;
}

static inline sinode*
si_plannerpeek_branch(siplanner *p, siplan *plan)
{
	srrbnode *pn;
	sinode *n;
	pn = sr_rbmax(&p->branch);
	while (pn) {
		n = srcast(pn, sinode, nodebranch);
		if (n->flags & SI_MERGE ||
		    n->flags & SI_BRANCH) {
			pn = sr_rbprev(&p->branch, pn);
			continue;
		}
		if (srunlikely(plan->condition & SI_BRANCH_FORCE))
			break;
		if ((plan->condition & SI_BRANCH_SIZE) && n->iused >= plan->a)
			break;
		if ((plan->condition & SI_BRANCH_LSN)) {
			if (n->i0.lsnmin <= plan->b)
				break;
			continue;
		}
		return NULL;
	}
	if (srunlikely(pn == NULL))
		return NULL;
	assert(! (n->flags & SI_MERGE));
	assert(! (n->flags & SI_BRANCH));
	n->flags |= SI_BRANCH;
	return n;
}

static inline sinode*
si_plannerpeek_merge(siplanner *p, siplan *plan)
{
	srrbnode *pn;
	sinode *n;
	pn = sr_rbmax(&p->merge);
	while (pn) {
		n = srcast(pn, sinode, nodemerge);
		if (n->flags & SI_MERGE ||
		    n->flags & SI_BRANCH) {
			pn = sr_rbprev(&p->merge, pn);
			continue;
		}
		if (srunlikely(plan->condition & SI_MERGE_FORCE))
			break;
		if ((plan->condition & SI_MERGE_DEEP) && n->lv >= plan->a)
			break;
		return NULL;
	}
	if (srunlikely(pn == NULL))
		return NULL;
	assert(! (n->flags & SI_MERGE));
	assert(! (n->flags & SI_BRANCH));
	n->flags |= SI_MERGE;
	return n;
}

sinode*
si_planner(siplanner *p, siplan *plan)
{
	switch (plan->plan) {
	case SI_BRANCH: return si_plannerpeek_branch(p, plan);
	case SI_MERGE:  return si_plannerpeek_merge(p, plan);
	}
	return NULL;
}

int si_plannerremove(siplanner *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		sr_rbremove(&p->branch, &n->nodebranch);
	if (mask & SI_MERGE)
		sr_rbremove(&p->merge, &n->nodemerge);
	return 0;
}

#if 0
void
si_plannerprint_branch(siplanner *p)
{
	srrbnode *pn;
	sinode *n;
	printf("BRANCH\n");
	pn = sr_rbmax(&p->branch);
	while (pn) {
		n = srcast(pn, sinode, nodebranch);
		if (n->flags == 0)
			break;
		printf("(%d) iused: %d, icount: %d, lv: %d, flags: ",
		       n->id.id, n->iused, n->icount, n->lv);
		if (n->flags & SI_BRANCH)
			printf("SI_BRANCH");
		if (n->flags & SI_MERGE)
			printf(" SI_MERGE");
		printf("\n");
		pn = sr_rbprev(&p->branch, pn);
	}
}

void
si_plannerprint_merge(siplanner *p)
{
	srrbnode *pn;
	sinode *n;
	printf("MERGE\n");
	pn = sr_rbmax(&p->merge);
	while (pn) {
		n = srcast(pn, sinode, nodemerge);
		if (n->flags == 0)
			break;
		printf("(%d) iused: %d, icount: %d, lv: %d, flags: ",
		       n->id.id, n->iused, n->icount, n->lv);
		if (n->flags & SI_BRANCH)
			printf("SI_BRANCH");
		if (n->flags & SI_MERGE)
			printf(" SI_MERGE");
		printf("\n");
		pn = sr_rbprev(&p->merge, pn);
	}
}
#endif
