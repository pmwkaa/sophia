
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
	/* 1Mb step up to 32Gb */
	int rc;
	rc = ss_rqinit(&p->memory, a, 1024 * 1024, 32000);
	if (ssunlikely(rc == -1))
		return -1;
	p->i = i;
	return 0;
}

int si_plannerfree(siplanner *p, ssa *a)
{
	ss_rqfree(&p->memory, a);
	return 0;
}

int si_plannertrace(siplan *p, uint32_t id, sstrace *t)
{
	char *plan = NULL;
	switch (p->plan) {
	case SI_COMPACTION: plan = "compaction";
		break;
	case SI_GC: plan = "gc";
		break;
	case SI_EXPIRE: plan = "expire";
		break;
	case SI_NODEGC: plan = "node gc";
		break;
	case SI_BACKUP:
	case SI_BACKUPEND: plan = "backup";
		break;
	}
	if (p->node) {
		ss_trace(t, "%s <%" PRIu32 ":%020" PRIu64 ".db>",
		         plan, id, p->node->id);
	} else {
		ss_trace(t, "%s <%" PRIu32 ">",
		         plan, id);
	}
	return 0;
}

int si_plannerupdate(siplanner *p, sinode *n)
{
	ss_rqupdate(&p->memory, &n->nodememory, n->used);
	return 0;
}

int si_plannerremove(siplanner *p, sinode *n)
{
	ss_rqdelete(&p->memory, &n->nodememory);
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
	while ((pn = ss_rqprev(&p->memory, pn))) {
		n = sscast(pn, sinode, nodememory);
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
si_plannerpeek_memory(siplanner *p, siplan *plan)
{
	/* try to peek a node with a biggest in-memory index */

	/* calculate peek wm */
	si *index = (si*)p->i;
	double cache_per_node =
		(double)index->scheme.compaction.cache /
		(double)index->n;
	if (cache_per_node >= index->scheme.compaction.node_size)
		cache_per_node = index->scheme.compaction.node_size;
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->memory, pn))) {
		n = sscast(pn, sinode, nodememory);
		if (n->flags & SI_LOCK)
			continue;
		if (n->used >= cache_per_node)
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
	/* try to peek a node with a biggest in-memory index
	 * which is ready for gc */
	siplannerrc rc = SI_PNONE;
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->memory, pn))) {
		n = sscast(pn, sinode, nodememory);
		sdindexheader *h = n->index.h;
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
	while ((pn = ss_rqprev(&p->memory, pn))) {
		n = sscast(pn, sinode, nodememory);
		sdindexheader *h = n->index.h;
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
	case SI_COMPACTION:
		return si_plannerpeek_memory(p, plan);
	case SI_NODEGC:
		return si_plannerpeek_nodegc(p, plan);
	case SI_GC:
		return si_plannerpeek_gc(p, plan);
	case SI_EXPIRE:
		return si_plannerpeek_expire(p, plan);
	case SI_BACKUP:
		return si_plannerpeek_backup(p, plan);
	}
	return -1;
}
