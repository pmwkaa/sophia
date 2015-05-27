
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
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

extern uint32_t si_vgc(ssa*, svv*);

static int
si_redistribute(si *index, sr *r, sdc *c, sinode *node, ssbuf *result,
                uint64_t vlsn)
{
	svindex *vindex = si_nodeindex(node);
	ssiter i;
	ss_iterinit(sv_indexiterraw, &i);
	ss_iteropen(sv_indexiterraw, &i, vindex);
	while (ss_iterhas(sv_indexiterraw, &i))
	{
		sv *v = ss_iterof(sv_indexiterraw, &i);
		int rc = ss_bufadd(&c->b, r->a, &v->v, sizeof(svv**));
		if (ssunlikely(rc == -1))
			return sr_malfunction(r->e, "%s", "memory allocation failed");
		ss_iternext(sv_indexiterraw, &i);
	}
	if (ssunlikely(ss_bufused(&c->b) == 0))
		return 0;
	uint32_t gc = 0;
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, &c->b, sizeof(svv*));
	ssiter j;
	ss_iterinit(ss_bufiterref, &j);
	ss_iteropen(ss_bufiterref, &j, result, sizeof(sinode*));
	sinode *prev = ss_iterof(ss_bufiterref, &j);
	ss_iternext(ss_bufiterref, &j);
	while (1)
	{
		sinode *p = ss_iterof(ss_bufiterref, &j);
		if (p == NULL) {
			assert(prev != NULL);
			while (ss_iterhas(ss_bufiterref, &i)) {
				svv *v = ss_iterof(ss_bufiterref, &i);
				v->next = NULL;

				svv *vgc = NULL;
				sv_indexset(&prev->i0, r, vlsn, v, &vgc);
				ss_iternext(ss_bufiterref, &i);
				if (vgc) {
					gc += si_vgc(r->a, vgc);
				}
			}
			break;
		}
		while (ss_iterhas(ss_bufiterref, &i))
		{
			svv *v = ss_iterof(ss_bufiterref, &i);
			v->next = NULL;

			svv *vgc = NULL;
			sdindexpage *page = sd_indexmin(&p->self.index);
			int rc = sr_compare(r->scheme, sv_vpointer(v), v->size,
			                    sd_indexpage_min(&p->self.index, page),
			                    page->sizemin);
			if (ssunlikely(rc >= 0))
				break;
			sv_indexset(&prev->i0, r, vlsn, v, &vgc);
			ss_iternext(ss_bufiterref, &i);
			if (vgc) {
				gc += si_vgc(r->a, vgc);
			}
		}
		if (ssunlikely(! ss_iterhas(ss_bufiterref, &i)))
			break;
		prev = p;
		ss_iternext(ss_bufiterref, &j);
	}
	if (gc) {
		ss_quota(index->quota, SS_QREMOVE, gc);
	}
	assert(ss_iterof(ss_bufiterref, &i) == NULL);
	return 0;
}

static inline void
si_redistribute_set(si *index, sr *r, uint64_t vlsn, uint64_t now, svv *v)
{
	index->update_time = now;
	/* match node */
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, r, index, SS_ROUTE, sv_vpointer(v), v->size);
	sinode *node = ss_iterof(si_iter, &i);
	assert(node != NULL);
	/* update node */
	svindex *vindex = si_nodeindex(node);
	svv *vgc = NULL;
	sv_indexset(vindex, r, vlsn, v, &vgc);
	node->update_time = index->update_time;
	node->used += sv_vsize(v);
	if (ssunlikely(vgc)) {
		uint32_t gc = si_vgc(r->a, vgc);
		node->used -= gc;
		ss_quota(index->quota, SS_QREMOVE, gc);
	}
	/* schedule node */
	si_plannerupdate(&index->p, SI_BRANCH, node);
}

static int
si_redistribute_index(si *index, sr *r, sdc *c, sinode *node, uint64_t vlsn)
{
	svindex *vindex = si_nodeindex(node);
	ssiter i;
	ss_iterinit(sv_indexiterraw, &i);
	ss_iteropen(sv_indexiterraw, &i, vindex);
	while (ss_iterhas(sv_indexiterraw, &i)) {
		sv *v = ss_iterof(sv_indexiterraw, &i);
		int rc = ss_bufadd(&c->b, r->a, &v->v, sizeof(svv**));
		if (ssunlikely(rc == -1))
			return sr_malfunction(r->e, "%s", "memory allocation failed");
		ss_iternext(sv_indexiterraw, &i);
	}
	if (ssunlikely(ss_bufused(&c->b) == 0))
		return 0;
	uint64_t now = ss_utime();
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, &c->b, sizeof(svv*));
	while (ss_iterhas(ss_bufiterref, &i)) {
		svv *v = ss_iterof(ss_bufiterref, &i);
		si_redistribute_set(index, r, vlsn, now, v);
		ss_iternext(ss_bufiterref, &i);
	}
	return 0;
}

static int
si_splitfree(ssbuf *result, sr *r)
{
	ssiter i;
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, result, sizeof(sinode*));
	while (ss_iterhas(ss_bufiterref, &i))
	{
		sinode *p = ss_iterof(ss_bufiterref, &i);
		si_nodefree(p, r, 0);
		ss_iternext(ss_bufiterref, &i);
	}
	return 0;
}

static inline int
si_split(si *index, sr *r, sdc *c, ssbuf *result,
         sinode   *parent,
         ssiter   *i,
         uint64_t  size_node,
         uint32_t  size_stream,
         uint64_t  vlsn)
{
	int count = 0;
	int rc;
	sdmergeconf mergeconf = {
		.size_stream     = size_stream,
		.size_node       = size_node,
		.size_page       = index->scheme->node_page_size,
		.checksum        = index->scheme->node_page_checksum,
		.compression     = index->scheme->compression,
		.compression_key = index->scheme->compression_key,
		.offset          = 0,
		.vlsn            = vlsn,
		.save_delete     = 0
	};
	sdmerge merge;
	sd_mergeinit(&merge, r, i, &c->build, &mergeconf);
	while ((rc = sd_merge(&merge)) > 0)
	{
		sinode *n = si_nodenew(r);
		if (ssunlikely(n == NULL))
			goto error;
		sdid id = {
			.parent = parent->self.id.id,
			.flags  = 0,
			.id     = sr_seq(r->seq, SR_NSNNEXT)
		};
		rc = sd_mergecommit(&merge, &id);
		if (ssunlikely(rc == -1))
			goto error;
		rc = si_nodecreate(n, r, index->scheme, &id, &merge.index, &c->build);
		if (ssunlikely(rc == -1))
			goto error;
		rc = ss_bufadd(result, r->a, &n, sizeof(sinode*));
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "%s", "memory allocation failed");
			si_nodefree(n, r, 1);
			goto error;
		}
		sd_buildreset(&c->build);
		count++;
	}
	if (ssunlikely(rc == -1))
		goto error;
	return 0;
error:
	si_splitfree(result, r);
	sd_mergefree(&merge);
	return -1;
}

int si_compaction(si *index, sr *r, sdc *c, uint64_t vlsn,
                  sinode *node,
                  ssiter *stream, uint32_t size_stream)
{
	ssbuf *result = &c->a;
	ssiter i;

	/* begin compaction.
	 *
	 * split merge stream into a number
	 * of a new nodes.
	 */
	int rc;
	rc = si_split(index, r, c, result,
	              node, stream,
	              index->scheme->node_size,
	              size_stream,
	              vlsn);
	if (ssunlikely(rc == -1))
		return -1;

	SS_INJECTION(r->i, SS_INJECTION_SI_COMPACTION_0,
	             si_splitfree(result, r);
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);

	/* mask removal of a single node as a
	 * single node update */
	int count = ss_bufused(result) / sizeof(sinode*);
	int count_index;

	si_lock(index);
	count_index = index->n;
	si_unlock(index);

	sinode *n;
	if (ssunlikely(count == 0 && count_index == 1))
	{
		n = si_bootstrap(index, r, node->self.id.id);
		if (ssunlikely(n == NULL))
			return -1;
		rc = ss_bufadd(result, r->a, &n, sizeof(sinode*));
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "%s", "memory allocation failed");
			si_nodefree(n, r, 1);
			return -1;
		}
		count++;
	}

	/* commit compaction changes */
	si_lock(index);
	svindex *j = si_nodeindex(node);
	si_plannerremove(&index->p, SI_COMPACT|SI_BRANCH, node);
	switch (count) {
	case 0: /* delete */
		si_remove(index, node);
		si_redistribute_index(index, r, c, node, vlsn);
		uint32_t used = sv_indexused(j);
		if (used) {
			ss_quota(index->quota, SS_QREMOVE, used);
		}
		break;
	case 1: /* self update */
		n = *(sinode**)result->s;
		n->i0   = *j;
		n->used = sv_indexused(j);
		si_nodelock(n);
		si_replace(index, node, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
		break;
	default: /* split */
		rc = si_redistribute(index, r, c, node, result, vlsn);
		if (ssunlikely(rc == -1)) {
			si_unlock(index);
			si_splitfree(result, r);
			return -1;
		}
		ss_iterinit(ss_bufiterref, &i);
		ss_iteropen(ss_bufiterref, &i, result, sizeof(sinode*));
		n = ss_iterof(ss_bufiterref, &i);
		n->used = sv_indexused(&n->i0);
		si_nodelock(n);
		si_replace(index, node, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
		for (ss_iternext(ss_bufiterref, &i); ss_iterhas(ss_bufiterref, &i);
		     ss_iternext(ss_bufiterref, &i)) {
			n = ss_iterof(ss_bufiterref, &i);
			n->used = sv_indexused(&n->i0);
			si_nodelock(n);
			si_insert(index, r, n);
			si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
		}
		break;
	}
	sv_indexinit(j);
	si_unlock(index);

	/* compaction completion */

	/* seal nodes */
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, result, sizeof(sinode*));
	while (ss_iterhas(ss_bufiterref, &i))
	{
		n = ss_iterof(ss_bufiterref, &i);
		if (index->scheme->sync) {
			rc = si_nodesync(n, r);
			if (ssunlikely(rc == -1))
				return -1;
		}
		rc = si_nodeseal(n, r, index->scheme);
		if (ssunlikely(rc == -1))
			return -1;
		SS_INJECTION(r->i, SS_INJECTION_SI_COMPACTION_3,
		             si_nodefree(node, r, 0);
		             sr_malfunction(r->e, "%s", "error injection");
		             return -1);
		ss_iternext(ss_bufiterref, &i);
	}

	SS_INJECTION(r->i, SS_INJECTION_SI_COMPACTION_1,
	             si_nodefree(node, r, 0);
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);

	/* gc old node */
	rc = si_nodefree(node, r, 1);
	if (ssunlikely(rc == -1))
		return -1;

	SS_INJECTION(r->i, SS_INJECTION_SI_COMPACTION_2,
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);

	/* complete new nodes */
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, result, sizeof(sinode*));
	while (ss_iterhas(ss_bufiterref, &i))
	{
		n = ss_iterof(ss_bufiterref, &i);
		rc = si_nodecomplete(n, r, index->scheme);
		if (ssunlikely(rc == -1))
			return -1;
		SS_INJECTION(r->i, SS_INJECTION_SI_COMPACTION_4,
		             sr_malfunction(r->e, "%s", "error injection");
		             return -1);
		ss_iternext(ss_bufiterref, &i);
	}

	/* unlock */
	si_lock(index);
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, result, sizeof(sinode*));
	while (ss_iterhas(ss_bufiterref, &i))
	{
		n = ss_iterof(ss_bufiterref, &i);
		si_nodeunlock(n);
		ss_iternext(ss_bufiterref, &i);
	}
	si_unlock(index);
	return 0;
}
