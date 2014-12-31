
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

int si_planinit(siplan *p)
{
	p->plan    = SI_NONE;
	p->explain = SI_ENONE;
	p->a       = 0;
	p->b       = 0;
	p->c       = 0;
	p->node    = NULL;
	return 0;
}

int si_plannerinit(siplanner *p)
{
	sr_rbinit(&p->branch);
	sr_rbinit(&p->compact);
	return 0;
}

int si_plannertrace(siplan *p, srtrace *t)
{
	char *plan = NULL;
	switch (p->plan) {
	case SI_BRANCH: plan = "branch";
		break;
	case SI_COMPACT: plan = "compact";
		break;
	case SI_CHECKPOINT: plan = "checkpoint";
		break;
	case SI_BACKUP: plan = "backup";
		break;
	}
	char *explain = NULL;;
	switch (p->explain) {
	case SI_ENONE:
		explain = "none";
		break;
	case SI_ERETRY:
		explain = "retry expected";
		break;
	case SI_EINDEX_SIZE:
		explain = "index size";
		break;
	case SI_EINDEX_TTL:
		explain = "index age";
		break;
	case SI_EBRANCH_COUNT:
		explain = "branch count";
		break;
	}
	sr_trace(t, "%s <#%" PRIu32 " explain: %s>",
	         plan,
	         p->node->self.id.id, explain);
	return 0;
}

srhot static inline int
si_plannercompact_cmp(sinode *a, sinode *b)
{
	if (a->branch_count != b->branch_count)
		return (a->branch_count > b->branch_count) ? 1 : -1;
	if (a->self.id.id == b->self.id.id)
		return 0;
	return (a->self.id.id > b->self.id.id) ? 1 : -1;
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
	if (a->self.id.id == b->self.id.id)
		return 0;
	return (a->self.id.id > b->self.id.id) ? 1 : -1;
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

static inline int
si_plannerpeek_ttl(siplanner *p, siplan *plan, srrbnode *pn, uint64_t now)
{
	sinode *n = NULL;
	for (; pn ; pn = sr_rbprev(&p->branch, pn)) {
		n = srcast(pn, sinode, nodebranch);
		if (n->flags & SI_LOCK)
			continue;
		if (n->used >= plan->c && ((now - n->update_time) >= plan->b))
			goto match;
	}
	return 0;
match:
	si_nodelock(n);
	plan->explain = SI_EINDEX_TTL;
	plan->node = n;
	return 1;
}

static inline int
si_plannerpeek_backup(siplanner *p, siplan *plan)
{
	/* try to peek a node which has
	 * bsn <= required value
	*/
	int rc_inprogress = 0;
	sinode *n;
	srrbnode *pn;
	pn = sr_rbmax(&p->branch);
	for (; pn ; pn = sr_rbprev(&p->branch, pn)) {
		n = srcast(pn, sinode, nodebranch);
		if (n->backup < plan->a) {
			if (n->flags & SI_LOCK) {
				rc_inprogress = 2;
				continue;
			}
			goto match;
		}
	}
	if (rc_inprogress)
		plan->explain = SI_ERETRY;
	return rc_inprogress;
match:
	si_nodelock(n);
	plan->explain = SI_ENONE;
	plan->node = n;
	return 1;
}

static inline int
si_plannerpeek_checkpoint(siplanner *p, siplan *plan)
{
	/* try to peek a node which has min
	 * lsn <= required value
	*/
	int rc_inprogress = 0;
	sinode *n;
	srrbnode *pn;
	pn = sr_rbmax(&p->branch);
	for (; pn ; pn = sr_rbprev(&p->branch, pn)) {
		n = srcast(pn, sinode, nodebranch);
		if (n->i0.lsnmin <= plan->a) {
			if (n->flags & SI_LOCK) {
				rc_inprogress = 2;
				continue;
			}
			goto match;
		}
	}
	if (rc_inprogress)
		plan->explain = SI_ERETRY;
	return rc_inprogress;
match:
	si_nodelock(n);
	plan->explain = SI_ENONE;
	plan->node = n;
	return 1;
}

static inline int
si_plannerpeek_branch(siplanner *p, siplan *plan)
{
	/* try to peek a node in the following order:
	 *
	 * a. has in-memory vindex size >= required value
	 * b. has last update time diff >= required value
	*/
	uint64_t now = sr_utime();
	sinode *n;
	srrbnode *pn;
	pn = sr_rbmax(&p->branch);
	for (; pn ; pn = sr_rbprev(&p->branch, pn)) {
		n = srcast(pn, sinode, nodebranch);
		if (n->flags & SI_LOCK)
			continue;
		if (n->used >= plan->a)
			goto match;
		/* continue to match a ttl-ready node */
		return si_plannerpeek_ttl(p, plan, pn, now);
	}
	return 0;
match:
	si_nodelock(n);
	plan->explain = SI_EINDEX_SIZE;
	plan->node = n;
	return 1;
}

static inline int
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
		if (n->branch_count >= plan->a)
			goto match;
		return 0;
	}
	return 0;
match:
	si_nodelock(n);
	plan->explain = SI_EBRANCH_COUNT;
	plan->node = n;
	return 1;
}

int si_planner(siplanner *p, siplan *plan)
{
	switch (plan->plan) {
	case SI_BRANCH:
		return si_plannerpeek_branch(p, plan);
	case SI_COMPACT:
		return si_plannerpeek_compact(p, plan);
	case SI_CHECKPOINT:
		return si_plannerpeek_checkpoint(p, plan);
	case SI_BACKUP:
		return si_plannerpeek_backup(p, plan);
	}
	return -1;
}

int si_plannerremove(siplanner *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		sr_rbremove(&p->branch, &n->nodebranch);
	if (mask & SI_COMPACT)
		sr_rbremove(&p->compact, &n->nodecompact);
	return 0;
}
