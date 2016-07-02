
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

int si_planinit(siplan *p)
{
	p->plan = SI_NONE;
	p->a    = 0;
	p->b    = 0;
	p->c    = 0;
	p->node = NULL;
	return 0;
}

int si_plannerinit(siplanner *p, ssa *a, void *i)
{
	int rc = ss_rqinit(&p->compact, a, 1, 4000);
	if (ssunlikely(rc == -1))
		return -1;
	/* 1Mb step up to 4Gb */
	rc = ss_rqinit(&p->branch, a, 1024 * 1024, 4000);
	if (ssunlikely(rc == -1)) {
		ss_rqfree(&p->compact, a);
		return -1;
	}
	rc = ss_rqinit(&p->temp, a, 1, 100);
	if (ssunlikely(rc == -1)) {
		ss_rqfree(&p->compact, a);
		ss_rqfree(&p->branch, a);
		return -1;
	}
	p->i = i;
	return 0;
}

int si_plannerfree(siplanner *p, ssa *a)
{
	ss_rqfree(&p->compact, a);
	ss_rqfree(&p->branch, a);
	ss_rqfree(&p->temp, a);
	return 0;
}

int si_plannertrace(siplan *p, uint32_t id, sstrace *t)
{
	char *plan = NULL;
	switch (p->plan) {
	case SI_BRANCH: plan = "branch";
		break;
	case SI_AGE: plan = "age";
		break;
	case SI_COMPACT: plan = "compact";
		break;
	case SI_CHECKPOINT: plan = "checkpoint";
		break;
	case SI_NODEGC: plan = "node gc";
		break;
	case SI_GC: plan = "gc";
		break;
	case SI_EXPIRE: plan = "expire";
		break;
	case SI_TEMP: plan = "temperature";
		break;
	case SI_BACKUP:
	case SI_BACKUPEND: plan = "backup";
		break;
	case SI_SNAPSHOT: plan = "snapshot";
		break;
	}
	if (p->node) {
		ss_trace(t, "%s <%" PRIu32 ":%020" PRIu64 ".db>",
		         plan, id, p->node->self.id.id);
	} else {
		ss_trace(t, "%s <%" PRIu32 ">",
		         plan, id);
	}
	return 0;
}

int si_plannerupdate(siplanner *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		ss_rqupdate(&p->branch, &n->nodebranch, n->used);
	if (mask & SI_COMPACT)
		ss_rqupdate(&p->compact, &n->nodecompact, n->branch_count);
	if (mask & SI_TEMP)
		ss_rqupdate(&p->temp, &n->nodetemp, n->temperature);
	return 0;
}

int si_plannerremove(siplanner *p, int mask, sinode *n)
{
	if (mask & SI_BRANCH)
		ss_rqdelete(&p->branch, &n->nodebranch);
	if (mask & SI_COMPACT)
		ss_rqdelete(&p->compact, &n->nodecompact);
	if (mask & SI_TEMP)
		ss_rqdelete(&p->temp, &n->nodetemp);
	return 0;
}

static inline siplannerrc
si_plannerpeek_backup(siplanner *p, siplan *plan)
{
	/* try to peek a node which has bsn <= required
	 * value
	*/
	siplannerrc rc = SI_PNONE;
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->branch, pn))) {
		n = sscast(pn, sinode, nodebranch);
		if (n->backup < plan->a) {
			if (n->flags & SI_LOCK) {
				rc = SI_PRETRY;
				continue;
			}
			goto match;
		}
	}
	if (rc)
		return rc;
	si *index = p->i;
	if (index->backup < plan->a) {
		plan->plan = SI_BACKUPEND;
		plan->node = 0;
	return SI_PMATCH;
	}
	return SI_PNONE;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_checkpoint(siplanner *p, siplan *plan)
{
	/* try to peek a node which has min
	 * lsn <= required value
	*/
	siplannerrc rc = SI_PNONE;
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->branch, pn))) {
		n = sscast(pn, sinode, nodebranch);
		if (n->i0.lsnmin <= plan->a) {
			if (n->flags & SI_LOCK) {
				rc = SI_PRETRY;
				continue;
			}
			goto match;
		}
	}
	return rc;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_branch(siplanner *p, siplan *plan)
{
	/* try to peek a node with a biggest in-memory index */
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->branch, pn))) {
		n = sscast(pn, sinode, nodebranch);
		if (n->flags & SI_LOCK)
			continue;
		if (n->used >= plan->a)
			goto match;
		return SI_PNONE;
	}
	return SI_PNONE;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_age(siplanner *p, siplan *plan)
{
	/* try to peek a node with update >= a and in-memory
	 * index size >= b */

	/* full scan */
	uint64_t now = ss_utime();
	sinode *n = NULL;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->branch, pn))) {
		n = sscast(pn, sinode, nodebranch);
		if (n->flags & SI_LOCK)
			continue;
		if (n->used >= plan->b && ((now - n->update_time) >= plan->a))
			goto match;
	}
	return SI_PNONE;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_compact(siplanner *p, siplan *plan)
{
	/* try to peek a node with a biggest number
	 * of branches */
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->compact, pn))) {
		n = sscast(pn, sinode, nodecompact);
		if (n->flags & SI_LOCK)
			continue;
		if (n->branch_count >= plan->a)
			goto match;
		return SI_PNONE;
	}
	return SI_PNONE;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_compact_temperature(siplanner *p, siplan *plan)
{
	/* try to peek a hottest node with number of
	 * branches >= watermark */
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->temp, pn))) {
		n = sscast(pn, sinode, nodetemp);
		if (n->flags & SI_LOCK)
			continue;
		if (n->branch_count >= plan->a)
			goto match;
		return SI_PNONE;
	}
	return SI_PNONE;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_gc(siplanner *p, siplan *plan)
{
	/* try to peek a node with a biggest number
	 * of branches which is ready for gc */
	siplannerrc rc = SI_PNONE;
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->compact, pn))) {
		n = sscast(pn, sinode, nodecompact);
		sdindexheader *h = n->self.index.h;
		if (sslikely(h->dupkeys == 0) || (h->dupmin >= plan->a))
			continue;
		uint32_t used = (h->dupkeys * 100) / h->keys;
		if (used >= plan->b) {
			if (n->flags & SI_LOCK) {
				rc = SI_PRETRY;
				continue;
			}
			goto match;
		}
	}
	return rc;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_expire(siplanner *p, siplan *plan)
{
	/* full scan */
	siplannerrc rc = SI_PNONE;
	uint32_t now = ss_timestamp();
	sinode *n = NULL;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->branch, pn))) {
		n = sscast(pn, sinode, nodebranch);
		sdindexheader *h = n->self.index.h;
		if (h->tsmin == UINT32_MAX)
			continue;
		uint32_t diff = now - h->tsmin;
		if (sslikely(diff >= plan->a)) {
			if (n->flags & SI_LOCK) {
				rc = SI_PRETRY;
				continue;
			}
			goto match;
		}
	}
	return rc;
match:
	si_nodelock(n);
	plan->node = n;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_snapshot(siplanner *p, siplan *plan)
{
	si *index = p->i;
	if (index->snapshot >= plan->a)
		return SI_PNONE;
	if (index->snapshot_run) {
		/* snaphot inprogress */
		return SI_PRETRY;
	}
	index->snapshot_run = 1;
	return SI_PMATCH;
}

static inline siplannerrc
si_plannerpeek_nodegc(siplanner *p, siplan *plan)
{
	si *index = p->i;
	if (sslikely(index->gc_count == 0))
		return 0;
	siplannerrc rc = SI_PNONE;
	sslist *i;
	ss_listforeach(&index->gc, i) {
		sinode *n = sscast(i, sinode, gc);
		if (sslikely(si_noderefof(n) == 0)) {
			ss_listunlink(&n->gc);
			index->gc_count--;
			plan->node = n;
			return SI_PMATCH;
		}
		rc = SI_PRETRY;
	}
	return rc;
}

siplannerrc
si_planner(siplanner *p, siplan *plan)
{
	switch (plan->plan) {
	case SI_BRANCH:
	case SI_COMPACT_INDEX:
		return si_plannerpeek_branch(p, plan);
	case SI_COMPACT:
		if (plan->b == 1)
			return si_plannerpeek_compact_temperature(p, plan);
		return si_plannerpeek_compact(p, plan);
	case SI_NODEGC:
		return si_plannerpeek_nodegc(p, plan);
	case SI_GC:
		return si_plannerpeek_gc(p, plan);
	case SI_EXPIRE:
		return si_plannerpeek_expire(p, plan);
	case SI_CHECKPOINT:
		return si_plannerpeek_checkpoint(p, plan);
	case SI_AGE:
		return si_plannerpeek_age(p, plan);
	case SI_BACKUP:
		return si_plannerpeek_backup(p, plan);
	case SI_SNAPSHOT:
		return si_plannerpeek_snapshot(p, plan);
	}
	return -1;
}
