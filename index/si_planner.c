
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
	sr_rbinit(&p->compact);
	return 0;
}

int si_plannertrace(siplan *plan, srtrace *t)
{
	if (plan->plan == SI_BRANCH) {
		sr_trace(t, "branch (node: %" PRIu32 ")", plan->node->id.id);
		return 0;
	}
	sr_trace(t, "compact (node: %" PRIu32 ")", plan->node->id.id);
	return 0;
}

srhot static inline int
si_plannercompact_cmp(sinode *a, sinode *b)
{
	if (a->lv != b->lv)
		return (a->lv > b->lv) ? 1 : -1;
	if (a->id.id == b->id.id)
		return 0;
	return (a->id.id > b->id.id) ? 1 : -1;
}

sr_rbget(si_plannercompact_match,
         si_plannercompact_cmp(srcast(n, sinode, nodecompact), (sinode*)key))

static inline int
si_plannercompact(siplanner *p, sinode *n)
{
	sr_rbremove(&p->compact, &n->nodecompact);
	srrbnode *pn = NULL;
	int rc = si_plannercompact_match(&p->compact, NULL, n, 0, &pn);
	assert(! (rc == 0 && pn));
	sr_rbset(&p->compact, pn, rc, &n->nodecompact);
#if 0
	pn = sr_rbmax(&p->compact);
	if (pn == NULL)
		return 0;
	n = srcast(pn, sinode, nodecompact);
	uint32_t lvlast = n->lv;
	pn = sr_rbprev(&p->compact, pn);
	while (pn) {
		n = srcast(pn, sinode, nodecompact);
		assert(n->lv <= lvlast);
		lvlast = n->lv;
		pn = sr_rbprev(&p->compact, pn);
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
	if (mask & SI_COMPACT)
		si_plannercompact(p, n);
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
		if (n->flags & SI_LOCK) {
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
	si_nodelock(n);
	return n;
}

static inline sinode*
si_plannerpeek_compact(siplanner *p, siplan *plan)
{
	srrbnode *pn;
	sinode *n;
	pn = sr_rbmax(&p->compact);
	while (pn) {
		n = srcast(pn, sinode, nodecompact);
		if (n->flags & SI_LOCK) {
			pn = sr_rbprev(&p->compact, pn);
			continue;
		}
		if (srunlikely(plan->condition & SI_COMPACT_FORCE))
			break;
		if ((plan->condition & SI_COMPACT_DEEP) && n->lv >= plan->a)
			break;
		return NULL;
	}
	if (srunlikely(pn == NULL))
		return NULL;
	si_nodelock(n);
	return n;
}

sinode*
si_planner(siplanner *p, siplan *plan)
{
	switch (plan->plan) {
	case SI_BRANCH: return si_plannerpeek_branch(p, plan);
	case SI_COMPACT:  return si_plannerpeek_compact(p, plan);
	}
	return NULL;
}

int si_plannerremove(siplanner *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		sr_rbremove(&p->branch, &n->nodebranch);
	if (mask & SI_COMPACT)
		sr_rbremove(&p->compact, &n->nodecompact);
	return 0;
}
