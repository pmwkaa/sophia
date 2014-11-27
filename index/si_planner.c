
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
	if (a->used != b->used)
		return (a->used > b->used) ? 1 : -1;
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
si_plannerpeek_branch_ttl(siplanner *p, siplan *plan, uint64_t time,
                          srrbnode *pn)
{
	for (; pn ; pn = sr_rbprev(&p->branch, pn)) {
		sinode *n = srcast(pn, sinode, nodebranch);
		if (n->flags & SI_LOCK)
			continue;
		if (n->used > 0 && ((time - n->update_time) >= plan->b))
			return n;
	}
	return NULL;
}

static inline sinode*
si_plannerpeek_branch(siplanner *p, siplan *plan)
{
	/* try to peek a node in the following order:
	 *
	 * a. has min lsn <= required value
	 * b. has in-memory vindex size >= required value
	 * c. has last update time diff >= required value
	*/
	uint64_t time = sr_utime();
	sinode *n_ttl = NULL;
	sinode *n;
	srrbnode *pn;
	pn = sr_rbmax(&p->branch);
	for (; pn ; pn = sr_rbprev(&p->branch, pn)) {
		n = srcast(pn, sinode, nodebranch);
		if (n->flags & SI_LOCK)
			continue;
		if (n_ttl == NULL) {
			if (n->used > 0 && ((time - n->update_time) >= plan->b))
				n_ttl = n;
		}
		if ((plan->condition & SI_CLSN)) {
			if (n->i0.lsnmin <= plan->c)
				goto match;
			continue;
		}
		if (n->used >= plan->a)
			goto match;
		/* continue to match a ttl-ready node */
		if (n_ttl == NULL)
			n_ttl = si_plannerpeek_branch_ttl(p, plan, time, pn);
		n = n_ttl;
		if (n)
			goto match;
		return NULL;
	}
	if (srunlikely(pn == NULL))
		return NULL;
match:
	si_nodelock(n);
	return n;
}

static inline sinode*
si_plannerpeek_compact(siplanner *p, siplan *plan)
{
	/* try to peek a node with a biggest number
	 * of branches */
	srrbnode *pn;
	sinode *n;
	pn = sr_rbmax(&p->compact);
	for (; pn ; pn = sr_rbprev(&p->compact, pn)) {
		n = srcast(pn, sinode, nodecompact);
		if (n->flags & SI_LOCK)
			continue;
		if (n->lv >= plan->a)
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
	case SI_BRANCH:
	case SI_COMPACT_INDEX:
		return si_plannerpeek_branch(p, plan);
	case SI_COMPACT: return si_plannerpeek_compact(p, plan);
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
