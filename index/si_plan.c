
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
	sr_rbinit(&p->branch);
	sr_rbinit(&p->merge);
	return 0;
}

srhot static inline int
si_planmerge_cmp(sinode *a, sinode *b)
{
	if (a->lv != b->lv)
		return (a->lv > b->lv) ? 1 : -1;
	if (a->id.id == b->id.id)
		return 0;
	return (a->id.id > b->id.id) ? 1 : -1;
}

sr_rbget(si_planmerge_match,
         si_planmerge_cmp(srcast(n, sinode, nodemerge), (sinode*)key))

static inline int
si_planmerge(siplan *p, sinode *n)
{
	sr_rbremove(&p->merge, &n->nodemerge);
	srrbnode *pn = NULL;
	int rc = si_planmerge_match(&p->merge, NULL, n, 0, &pn);
	assert(! (rc == 0 && pn));
	sr_rbset(&p->merge, pn, rc, &n->nodemerge);

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
	return 0;
}

srhot static inline int
si_planbranch_cmp(sinode *a, sinode *b)
{
	if (a->iused != b->iused)
		return (a->iused > b->iused) ? 1 : -1;
	if (a->id.id == b->id.id)
		return 0;
	return (a->id.id > b->id.id) ? 1 : -1;
}

sr_rbget(si_planbranch_match,
         si_planbranch_cmp(srcast(n, sinode, nodebranch), (sinode*)key))

static inline int
si_planbranch(siplan *p, sinode *n)
{
	sr_rbremove(&p->branch, &n->nodebranch);
	srrbnode *pn = NULL;
	int rc = si_planbranch_match(&p->branch, NULL, n, 0, &pn);
	assert(! (rc == 0 && pn));
	sr_rbset(&p->branch, pn, rc, &n->nodebranch);

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
	return 0;
}

int si_plan(siplan *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		si_planbranch(p, n);
	if (mask & SI_MERGE)
		si_planmerge(p, n);
	return 0;
}

static inline sinode*
si_planpeek_branch(siplan *p, uint32_t wm)
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
		if (n->iused < wm)
			return NULL;
		break;
	}
	if (srunlikely(pn == NULL))
		return NULL;
	assert(! (n->flags & SI_MERGE));
	assert(! (n->flags & SI_BRANCH));
	n->flags |= SI_BRANCH;
	return n;
}

static inline sinode*
si_planpeek_merge(siplan *p, uint32_t wm)
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
		if (n->lv < wm)
			return NULL;
		break;
	}
	if (srunlikely(pn == NULL))
		return NULL;
	assert(! (n->flags & SI_MERGE));
	assert(! (n->flags & SI_BRANCH));
	n->flags |= SI_MERGE;
	return n;
}

sinode*
si_planpeek(siplan *p, int op, uint32_t wm)
{
	switch (op) {
	case SI_BRANCH: return si_planpeek_branch(p, wm);
	case SI_MERGE:  return si_planpeek_merge(p, wm);
	}
	return NULL;
}

int si_planremove(siplan *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		sr_rbremove(&p->branch, &n->nodebranch);
	if (mask & SI_MERGE)
		sr_rbremove(&p->merge, &n->nodemerge);
	return 0;
}

void
si_planprint_branch(siplan *p)
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
si_planprint_merge(siplan *p)
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
