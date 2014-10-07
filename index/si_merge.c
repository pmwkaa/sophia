
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
si_redistribute(sr *r, sdc *c, sinode *node, srbuf *result, uint64_t lsvn)
{
	sriter i;
	sr_iterinit(&i, &sv_indexiterraw, r);
	sr_iteropen(&i, &node->i0);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		int rc = sr_bufadd(&c->c, r->a, &v->v, sizeof(svv**));
		if (srunlikely(rc == -1))
			return -1;
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
				sv_indexset(&prev->i0, r, lsvn, v, &vgc);
				prev->iused += sv_vsize(v);
				prev->iusedkv += v->keysize + v->valuesize;
				prev->icount++;
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
			sv_indexset(&prev->i0, r, lsvn, v, &vgc);
			prev->iused += sv_vsize(v);
			prev->iusedkv += v->keysize + v->valuesize;
			prev->icount++;
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
	assert(sr_iterof(&i) == NULL);
	return 0;
}

static inline int
si_mergeof(si *index, sr *r, sdc *c, sinode *node,
           sriter *stream,
           uint32_t size_stream,
           uint32_t size_key)
{
	srbuf *result = &c->a;
	sriter i;
	/* create nodes */
	sisplit s = {
		.root         = node,
		.src          = node,
		.src_deriveid = 0,
		.i            = stream,
		.size_key     = size_key,
		.size_stream  = size_stream,
		.size_node    = index->conf->node_size,
		.conf         = index->conf,
		.lsvn         = sr_seq(r->seq, SR_LSN) - 1,
	};
	int rc = si_split(&s, r, c, result);
	if (srunlikely(rc == -1))
		return -1;
	int count = sr_bufused(result) / sizeof(sinode*);
	assert(count >= 1);

	si_lock(index);
	si_planremove(&index->plan, SI_MERGE|SI_BRANCH, node);
	sinode *n;
	if (srlikely(count == 1)) {
		n = *(sinode**)result->s;
		n->iused   = node->iused;
		n->iusedkv = node->iusedkv;
		n->icount  = node->icount;
		n->i0      = node->i0;
		n->flags  |= SI_MERGE|SI_BRANCH;
		sv_indexinit(&node->i0);
		si_replace(index, node, n);
		si_plan(&index->plan, SI_MERGE|SI_BRANCH, n);
	} else {
		rc = si_redistribute(r, c, node, result, s.lsvn);
		if (srunlikely(rc == -1)) {
			si_unlock(index);
			si_splitfree(result, r);
			return -1;
		}
		sv_indexinit(&node->i0);
		sr_iterinit(&i, &sr_bufiterref, NULL);
		sr_iteropen(&i, result, sizeof(sinode*));
		n = sr_iterof(&i);
		n->flags |= SI_MERGE|SI_BRANCH;
		si_replace(index, node, n);
		si_plan(&index->plan, SI_MERGE|SI_BRANCH, n);
		for (sr_iternext(&i); sr_iterhas(&i);
			 sr_iternext(&i)) {
			n = sr_iterof(&i);
			n->flags |= SI_MERGE|SI_BRANCH;
			si_insert(index, r, n);
			si_plan(&index->plan, SI_MERGE|SI_BRANCH, n);
		}
	}
	si_unlock(index);

	/* garbage collection */

	/* remove old files */
	rc = si_nodegc(node, r);
	if (srunlikely(rc == -1))
		return -1;
	/* rename new nodes */
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, result, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		n = sr_iterof(&i);
		rc = si_nodeseal(n, index->conf, NULL);
		if (srunlikely(rc == -1))
			return -1;
	}
	/* complete */
	si_lock(index);
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, result, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		n = sr_iterof(&i);
		n->flags &= ~SI_BRANCH;
		n->flags &= ~SI_MERGE;
	}
	si_unlock(index);
	return 0;
}

static inline int
si_mergeadd(svmerge *m, sr *r, sinode *n,
            uint32_t *size_stream,
            uint32_t *size_key)
{
	svmergesrc *src = sv_mergeadd(m);
	if (srunlikely(src == NULL))
		return -1;
	uint16_t key = sd_indexkeysize(&n->index);
	if (key > *size_key)
		*size_key = key;
	*size_stream += sd_indextotal_kv(&n->index);
	sr_iterinit(&src->i, &sd_iter, r);
	sr_iteropen(&src->i, &n->map, 0);
	return 0;
}

int si_merge(si *index, sr *r, sdc *c, uint32_t wm)
{
	si_lock(index);
	sinode *node = si_planpeek(&index->plan, SI_MERGE, wm);
	if (srunlikely(node == NULL)) {
		si_unlock(index);
		return 0;
	}
	/*si_planprint_merge(&index->plan);*/
	si_unlock(index);
	sd_creset(c);
	svmerge merge;
	sv_mergeinit(&merge);
	int rc = sv_mergeprepare(&merge, r->a, node->lv + 1, 0);
	if (srunlikely(rc == -1))
		return -1;
	uint32_t size_stream = 0;
	uint32_t size_key = 0;
	sinode *n;
	for (n = node->next; n; n = n->next)
		si_mergeadd(&merge, r, n, &size_stream, &size_key);
	si_mergeadd(&merge, r, node, &size_stream, &size_key);
	sriter i;
	sr_iterinit(&i, &sv_mergeiter, r);
	sr_iteropen(&i, &merge, SR_GTE);
	rc = si_mergeof(index, r, c, node, &i, size_stream, size_key);
	if (srunlikely(rc == -1)) {
		sv_mergefree(&merge, r->a);
		return -1;
	}
	sv_mergefree(&merge, r->a);
	return 0;
}
