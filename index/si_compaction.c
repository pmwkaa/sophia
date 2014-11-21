
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

extern void si_vgc(sra*, svv*);

static int
si_redistribute(sr *r, sdc *c, sinode *node, srbuf *result, uint64_t vlsn)
{
	svindex *index = si_nodeindex(node);
	sriter i;
	sr_iterinit(&i, &sv_indexiterraw, r);
	sr_iteropen(&i, index);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		int rc = sr_bufadd(&c->c, r->a, &v->v, sizeof(svv**));
		if (srunlikely(rc == -1))
			return sr_error(r->e, "%s", "memory allocation failed");
	}
	if (srunlikely(sr_bufused(&c->c) == 0))
		return 0;
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, &c->c, sizeof(svv*));
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
				svv *vgc = NULL;
				sv_indexset(&prev->i0, r, vlsn, v, &vgc);
				prev->iused += sv_vsize(v);
				prev->iusedkv += v->keysize + v->valuesize;
				sr_iternext(&i);
				if (vgc) {
					si_vgc(r->a, vgc);
				}
			}
			break;
		}
		while (sr_iterhas(&i)) {
			svv *v = sr_iterof(&i);
			svv *vgc = NULL;
			sdindexpage *page = sd_indexmin(&p->index);
			int rc = sr_compare(r->cmp, sv_vkey(v), v->keysize,
			                    sd_indexpage_min(page), page->sizemin);
			if (srunlikely(rc >= 0))
				break;
			sv_indexset(&prev->i0, r, vlsn, v, &vgc);
			prev->iused += sv_vsize(v);
			prev->iusedkv += v->keysize + v->valuesize;
			sr_iternext(&i);
			if (vgc) {
				si_vgc(r->a, vgc);
			}
		}
		if (srunlikely(! sr_iterhas(&i)))
			break;
		prev = p;
		sr_iternext(&j);
	}
	/* xxx: qos from vgc */
	assert(sr_iterof(&i) == NULL);
	return 0;
}

int
si_compaction(si *index, sr *r, sdc *c, uint64_t vlsn,
              sinode *node,
              sriter *stream,
              uint32_t size_stream,
              uint32_t size_key)
{
	srbuf *result = &c->a;
	sriter i;

	/* split merge stream into a number of
	 * a new nodes */
	sisplit s = {
		.parent       = node,
		.flags        = 0,
		.i            = stream,
		.size_key     = size_key,
		.size_stream  = size_stream,
		.size_node    = index->conf->node_size,
		.conf         = index->conf,
		.vlsn         = vlsn
	};
	int rc = si_split(&s, r, c, result);
	if (srunlikely(rc == -1))
		return -1;
	int count = sr_bufused(result) / sizeof(sinode*);
	assert(count >= 1);

	SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_0,
	             si_splitfree(result, r);
	             sr_error(r->e, "%s", "error injection");
	             return -1);

	si_lock(index);
	svindex *j = si_nodeindex(node);
	si_plannerremove(&index->p, SI_COMPACT|SI_BRANCH, node);
	sinode *n;
	if (srlikely(count == 1)) {
		n = *(sinode**)result->s;
		/* xxx: set n->iused using j->used */
		n->iused   = node->iused;
		n->iusedkv = node->iusedkv;
		n->i0      = *j;
		si_nodelock(n);
		si_replace(index, node, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
	} else {
		rc = si_redistribute(r, c, node, result, s.vlsn);
		if (srunlikely(rc == -1)) {
			si_unlock(index);
			si_splitfree(result, r);
			return -1;
		}
		sr_iterinit(&i, &sr_bufiterref, NULL);
		sr_iteropen(&i, result, sizeof(sinode*));
		n = sr_iterof(&i);
		si_nodelock(n);
		si_replace(index, node, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
		for (sr_iternext(&i); sr_iterhas(&i);
			 sr_iternext(&i)) {
			n = sr_iterof(&i);
			si_nodelock(n);
			si_insert(index, r, n);
			si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
		}
	}
	sv_indexinit(j);
	si_unlock(index);

	/* garbage collection */

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
		             si_nodefree_all(node, r);
		             sr_error(r->e, "%s", "error injection");
		             return -1);
	}

	SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_1,
	             si_nodefree_all(node, r);
	             sr_error(r->e, "%s", "error injection");
	             return -1);

	/* remove old files */
	rc = si_nodegc(node, r);
	if (srunlikely(rc == -1))
		return -1;

	SR_INJECTION(r->i, SR_INJECTION_SI_COMPACTION_2,
	             sr_error(r->e, "%s", "error injection");
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
		             sr_error(r->e, "%s", "error injection");
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
