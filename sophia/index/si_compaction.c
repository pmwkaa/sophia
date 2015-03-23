
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

extern uint32_t si_vgc(sra*, svv*);

static int
si_redistribute(si *index, sr *r, sdc *c, sinode *node, srbuf *result,
                uint64_t vlsn)
{
	svindex *vindex = si_nodeindex(node);
	sriter i;
	sr_iterinit(&i, &sv_indexiterraw, r);
	sr_iteropen(&i, vindex);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		int rc = sr_bufadd(&c->b, r->a, &v->v, sizeof(svv**));
		if (srunlikely(rc == -1))
			return sr_malfunction(r->e, "%s", "memory allocation failed");
	}
	if (srunlikely(sr_bufused(&c->b) == 0))
		return 0;
	uint32_t gc = 0;
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, &c->b, sizeof(svv*));
	sriter j;
	sr_iterinit(&j, &sr_bufiterref, NULL);
	sr_iteropen(&j, result, sizeof(sinode*));
	sinode *prev = sr_iterof(&j);
	sr_iternext(&j);
	while (1) {
		sinode *p = sr_iterof(&j);
		if (p == NULL) {
			assert(prev != NULL);
			while (sr_iterhas(&i)) {
				svv *v = sr_iterof(&i);
				v->next = NULL;

				svv *vgc = NULL;
				sv_indexset(&prev->i0, r, vlsn, v, &vgc);
				sr_iternext(&i);
				if (vgc) {
					gc += si_vgc(r->a, vgc);
				}
			}
			break;
		}
		while (sr_iterhas(&i)) {
			svv *v = sr_iterof(&i);
			v->next = NULL;

			svv *vgc = NULL;
			sdindexpage *page = sd_indexmin(&p->self.index);
			int rc = sr_compare(r->cmp, sv_vkey(v), v->keysize,
			                    sd_indexpage_min(page), page->sizemin);
			if (srunlikely(rc >= 0))
				break;
			sv_indexset(&prev->i0, r, vlsn, v, &vgc);
			sr_iternext(&i);
			if (vgc) {
				gc += si_vgc(r->a, vgc);
			}
		}
		if (srunlikely(! sr_iterhas(&i)))
			break;
		prev = p;
		sr_iternext(&j);
	}
	if (gc) {
		sr_quota(index->quota, SR_QREMOVE, gc);
	}
	assert(sr_iterof(&i) == NULL);
	return 0;
}

static inline void
si_redistribute_set(si *index, sr *r, uint64_t vlsn, uint64_t now, svv *v)
{
	index->update_time = now;
	/* match node */
	sriter i;
	sr_iterinit(&i, &si_iter, r);
	sr_iteropen(&i, index, SR_ROUTE, sv_vkey(v), v->keysize);
	sinode *node = sr_iterof(&i);
	assert(node != NULL);
	/* update node */
	svindex *vindex = si_nodeindex(node);
	svv *vgc = NULL;
	sv_indexset(vindex, r, vlsn, v, &vgc);
	node->update_time = index->update_time;
	node->used += sv_vsize(v);
	if (srunlikely(vgc)) {
		uint32_t gc = si_vgc(r->a, vgc);
		node->used -= gc;
		sr_quota(index->quota, SR_QREMOVE, gc);
	}
	/* schedule node */
	si_plannerupdate(&index->p, SI_BRANCH, node);
}

static int
si_redistribute_index(si *index, sr *r, sdc *c, sinode *node, uint64_t vlsn)
{
	svindex *vindex = si_nodeindex(node);
	sriter i;
	sr_iterinit(&i, &sv_indexiterraw, r);
	sr_iteropen(&i, vindex);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		int rc = sr_bufadd(&c->b, r->a, &v->v, sizeof(svv**));
		if (srunlikely(rc == -1))
			return sr_malfunction(r->e, "%s", "memory allocation failed");
	}
	if (srunlikely(sr_bufused(&c->b) == 0))
		return 0;
	uint64_t now = sr_utime();
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, &c->b, sizeof(svv*));
	while (sr_iterhas(&i)) {
		svv *v = sr_iterof(&i);
		si_redistribute_set(index, r, vlsn, now, v);
		sr_iternext(&i);
	}
	return 0;
}

static int
si_splitfree(srbuf *result, sr *r)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, result, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sinode *p = sr_iterof(&i);
		si_nodefree(p, r, 0);
	}
	return 0;
}

static inline int
si_split(si *index, sr *r, sdc *c, srbuf *result,
         sinode   *parent,
         sriter   *i,
         uint64_t  size_node,
         uint32_t  size_key,
         uint32_t  size_stream,
         uint64_t  vlsn)
{
	int count = 0;
	int rc;
	sdmerge merge;
	sd_mergeinit(&merge, r, parent->self.id.id,
	             i, &c->build,
	             0, /* offset */
	             size_key,
	             size_stream,
	             size_node,
	             index->conf->node_page_size,
	             index->conf->node_page_checksum,
	             0, vlsn);
	while ((rc = sd_merge(&merge)) > 0)
	{
		sinode *n = si_nodenew(r);
		if (srunlikely(n == NULL))
			goto error;
		sdid id = {
			.parent = parent->self.id.id,
			.flags  = 0,
			.id     = sr_seq(r->seq, SR_NSNNEXT)
		};
		rc = sd_mergecommit(&merge, &id);
		if (srunlikely(rc == -1))
			goto error;
		rc = si_nodecreate(n, r, index->conf, &id, &merge.index, &c->build);
		if (srunlikely(rc == -1))
			goto error;
		rc = sr_bufadd(result, r->a, &n, sizeof(sinode*));
		if (srunlikely(rc == -1)) {
			sr_malfunction(r->e, "%s", "memory allocation failed");
			si_nodefree(n, r, 1);
			goto error;
		}
		sd_buildreset(&c->build);
		count++;
	}
	if (srunlikely(rc == -1))
		goto error;
	return 0;
error:
	si_splitfree(result, r);
	sd_mergefree(&merge);
	return -1;
}

int si_compaction(si *index, sr *r, sdc *c, uint64_t vlsn,
                  sinode *node,
                  sriter *stream,
                  uint32_t size_stream,
                  uint32_t size_key)
{
	srbuf *result = &c->a;
	sriter i;

	/* begin compaction.
	 *
	 * split merge stream into a number
	 * of a new nodes.
	 */
	int rc;
	rc = si_split(index, r, c, result,
	              node, stream,
	              index->conf->node_size,
	              size_key,
	              size_stream,
	              vlsn);
	if (srunlikely(rc == -1))
		return -1;

	SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_0,
	             si_splitfree(result, r);
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);

	/* mask removal of a single node as a
	 * single node update */
	int count = sr_bufused(result) / sizeof(sinode*);
	int count_index;

	si_lock(index);
	count_index = index->n;
	si_unlock(index);

	sinode *n;
	if (srunlikely(count == 0 && count_index == 1))
	{
		n = si_bootstrap(index, r, node->self.id.id);
		if (srunlikely(n == NULL))
			return -1;
		rc = sr_bufadd(result, r->a, &n, sizeof(sinode*));
		if (srunlikely(rc == -1)) {
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
			sr_quota(index->quota, SR_QREMOVE, used);
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
		if (srunlikely(rc == -1)) {
			si_unlock(index);
			si_splitfree(result, r);
			return -1;
		}
		sr_iterinit(&i, &sr_bufiterref, NULL);
		sr_iteropen(&i, result, sizeof(sinode*));
		n = sr_iterof(&i);
		n->used = sv_indexused(&n->i0);
		si_nodelock(n);
		si_replace(index, node, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
		for (sr_iternext(&i); sr_iterhas(&i);
		     sr_iternext(&i)) {
			n = sr_iterof(&i);
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
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, result, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		n = sr_iterof(&i);
		if (index->conf->sync) {
			rc = si_nodesync(n, r);
			if (srunlikely(rc == -1))
				return -1;
		}
		rc = si_nodeseal(n, r, index->conf);
		if (srunlikely(rc == -1))
			return -1;
		SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_3,
		             si_nodefree(node, r, 0);
		             sr_malfunction(r->e, "%s", "error injection");
		             return -1);
	}

	SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_1,
	             si_nodefree(node, r, 0);
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);

	/* gc old node */
	rc = si_nodefree(node, r, 1);
	if (srunlikely(rc == -1))
		return -1;

	SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_2,
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);

	/* complete new nodes */
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, result, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		n = sr_iterof(&i);
		rc = si_nodecomplete(n, r, index->conf);
		if (srunlikely(rc == -1))
			return -1;
		SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_4,
		             sr_malfunction(r->e, "%s", "error injection");
		             return -1);
	}

	/* unlock */
	si_lock(index);
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, result, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		n = sr_iterof(&i);
		si_nodeunlock(n);
	}
	si_unlock(index);
	return 0;
}
