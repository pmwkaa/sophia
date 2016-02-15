
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

static int
si_redistribute(si *index, sr *r, sdc *c, sinode *node, ssbuf *result)
{
	(void)index;
	svindex *vindex = si_nodeindex(node);
	ssiter i;
	ss_iterinit(sv_indexiter, &i);
	ss_iteropen(sv_indexiter, &i, r, vindex, SS_GTE, NULL, 0);
	while (ss_iterhas(sv_indexiter, &i))
	{
		sv *v = ss_iterof(sv_indexiter, &i);
		int rc = ss_bufadd(&c->b, r->a, &v->v, sizeof(svref**));
		if (ssunlikely(rc == -1))
			return sr_oom_malfunction(r->e);
		ss_iternext(sv_indexiter, &i);
	}
	if (ssunlikely(ss_bufused(&c->b) == 0))
		return 0;
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, &c->b, sizeof(svref*));
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
				svref *v = ss_iterof(ss_bufiterref, &i);
				v->next = NULL;
				sv_indexset(&prev->i0, r, v);
				ss_iternext(ss_bufiterref, &i);
			}
			break;
		}
		while (ss_iterhas(ss_bufiterref, &i))
		{
			svref *v = ss_iterof(ss_bufiterref, &i);
			v->next = NULL;
			sdindexpage *page = sd_indexmin(&p->self.index);
			int rc = sr_compare(r->scheme, sv_vpointer(v->v), v->v->size,
			                    sd_indexpage_min(&p->self.index, page),
			                    page->sizemin);
			if (ssunlikely(rc >= 0))
				break;
			sv_indexset(&prev->i0, r, v);
			ss_iternext(ss_bufiterref, &i);
		}
		if (ssunlikely(! ss_iterhas(ss_bufiterref, &i)))
			break;
		prev = p;
		ss_iternext(ss_bufiterref, &j);
	}
	assert(ss_iterof(ss_bufiterref, &i) == NULL);
	return 0;
}

static inline void
si_redistribute_set(si *index, sr *r, uint64_t now, svref *v)
{
	index->update_time = now;
	/* match node */
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, r, index, SS_GTE, sv_vpointer(v->v), v->v->size);
	sinode *node = ss_iterof(si_iter, &i);
	assert(node != NULL);
	/* update node */
	svindex *vindex = si_nodeindex(node);
	sv_indexset(vindex, r, v);
	node->update_time = index->update_time;
	node->used += sv_vsize(v->v);
	/* schedule node */
	si_plannerupdate(&index->p, SI_BRANCH, node);
}

static int
si_redistribute_index(si *index, sr *r, sdc *c, sinode *node)
{
	svindex *vindex = si_nodeindex(node);
	ssiter i;
	ss_iterinit(sv_indexiter, &i);
	ss_iteropen(sv_indexiter, &i, r, vindex, SS_GTE, NULL, 0);
	while (ss_iterhas(sv_indexiter, &i)) {
		sv *v = ss_iterof(sv_indexiter, &i);
		int rc = ss_bufadd(&c->b, r->a, &v->v, sizeof(svref**));
		if (ssunlikely(rc == -1))
			return sr_oom_malfunction(r->e);
		ss_iternext(sv_indexiter, &i);
	}
	if (ssunlikely(ss_bufused(&c->b) == 0))
		return 0;
	uint64_t now = ss_utime();
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, &c->b, sizeof(svref*));
	while (ss_iterhas(ss_bufiterref, &i)) {
		svref *v = ss_iterof(ss_bufiterref, &i);
		v->next = NULL;
		si_redistribute_set(index, r, now, v);
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
si_split(si *index, sdc *c, ssbuf *result,
         sinode   *parent,
         ssiter   *i,
         uint64_t  size_node,
         uint64_t  size_stream,
         uint32_t  stream,
         uint64_t  vlsn,
         uint64_t  vlsn_lru)
{
	sr *r = index->r;
	int rc;
	sdmergeconf mergeconf = {
		.stream          = stream,
		.size_stream     = size_stream,
		.size_node       = size_node,
		.size_page       = index->scheme->node_page_size,
		.checksum        = index->scheme->node_page_checksum,
		.compression_key = index->scheme->compression_key,
		.compression     = index->scheme->compression,
		.compression_if  = index->scheme->compression_if,
		.amqf            = index->scheme->amqf,
		.vlsn            = vlsn,
		.vlsn_lru        = vlsn_lru,
		.save_delete     = 0,
		.save_upsert     = 0,
		.save_set        = !index->scheme->cache_mode
	};
	sinode *n = NULL;
	sdmerge merge;
	rc = sd_mergeinit(&merge, r, i, &c->build, &c->qf, &c->upsert, &mergeconf);
	if (ssunlikely(rc == -1))
		return -1;
	while ((rc = sd_merge(&merge)) > 0)
	{
		/* create new node */
		n = si_nodenew(r);
		if (ssunlikely(n == NULL))
			goto error;
		sdid id = {
			.parent = parent->self.id.id,
			.flags  = 0,
			.id     = sr_seq(index->r->seq, SR_NSNNEXT)
		};
		rc = si_nodecreate(n, r, index->scheme, &id);
		if (ssunlikely(rc == -1))
			goto error;
		n->branch = &n->self;
		n->branch_count++;

		ssblob *blob = NULL;
		if (parent->in_memory) {
			blob = &n->self.copy;
			rc = ss_blobensure(blob, index->scheme->node_size);
			if (ssunlikely(rc == -1))
				goto error;
			n->in_memory = 1;
		}

		/* write open seal */
		uint64_t seal = n->file.size;
		rc = sd_writeseal(r, &n->file, blob);
		if (ssunlikely(rc == -1))
			goto error;

		/* write pages */
		uint64_t offset = n->file.size;
		while ((rc = sd_mergepage(&merge, offset)) == 1) {
			rc = sd_writepage(r, &n->file, blob, merge.build);
			if (ssunlikely(rc == -1))
				goto error;
			offset = n->file.size;
		}
		if (ssunlikely(rc == -1))
			goto error;

		rc = sd_mergecommit(&merge, &id, n->file.size);
		if (ssunlikely(rc == -1))
			goto error;

		/* write index */
		rc = sd_writeindex(r, &n->file, blob, &merge.index);
		if (ssunlikely(rc == -1))
			goto error;

		/* update seal */
		rc = sd_seal(r, &n->file, blob, &merge.index, seal);
		if (ssunlikely(rc == -1))
			goto error;

		/* in-memory mode */
		if (blob) {
			rc = ss_blobfit(blob);
			if (ssunlikely(rc == -1))
				goto error;
		}
		/* mmap mode */
		if (index->scheme->mmap) {
			rc = si_nodemap(n, r);
			if (ssunlikely(rc == -1))
				goto error;
		}

		/* add node to the list */
		rc = ss_bufadd(result, index->r->a, &n, sizeof(sinode*));
		if (ssunlikely(rc == -1)) {
			sr_oom_malfunction(index->r->e);
			goto error;
		}

		si_branchset(&n->self, &merge.index);
	}
	if (ssunlikely(rc == -1))
		goto error;
	return 0;
error:
	if (n)
		si_nodefree(n, r, 0);
	sd_mergefree(&merge);
	si_splitfree(result, r);
	return -1;
}

int si_merge(si *index, sdc *c, sinode *node,
             uint64_t vlsn,
             uint64_t vlsn_lru,
             ssiter *stream,
             uint64_t size_stream,
             uint32_t n_stream)
{
	sr *r = index->r;
	ssbuf *result = &c->a;
	ssiter i;

	/* begin compaction.
	 *
	 * Split merge stream into a number of
	 * a new nodes.
	 */
	int rc;
	rc = si_split(index, c, result,
	              node, stream,
	              index->scheme->node_size,
	              size_stream,
	              n_stream,
	              vlsn,
	              vlsn_lru);
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
		n = si_bootstrap(index, node->self.id.id);
		if (ssunlikely(n == NULL))
			return -1;
		rc = ss_bufadd(result, r->a, &n, sizeof(sinode*));
		if (ssunlikely(rc == -1)) {
			sr_oom_malfunction(r->e);
			si_nodefree(n, r, 1);
			return -1;
		}
		count++;
	}

	/* commit compaction changes */
	si_lock(index);
	svindex *j = si_nodeindex(node);
	si_plannerremove(&index->p, SI_COMPACT|SI_BRANCH|SI_TEMP, node);
	index->size -= si_nodesize(node);
	switch (count) {
	case 0: /* delete */
		si_remove(index, node);
		si_redistribute_index(index, r, c, node);
		break;
	case 1: /* self update */
		n = *(sinode**)result->s;
		n->i0 = *j;
		n->temperature = node->temperature;
		n->temperature_reads = node->temperature_reads;
		n->used = sv_indexused(j);
		index->size += si_nodesize(n);
		si_nodelock(n);
		si_replace(index, node, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH|SI_TEMP, n);
		break;
	default: /* split */
		rc = si_redistribute(index, r, c, node, result);
		if (ssunlikely(rc == -1)) {
			si_unlock(index);
			si_splitfree(result, r);
			return -1;
		}
		ss_iterinit(ss_bufiterref, &i);
		ss_iteropen(ss_bufiterref, &i, result, sizeof(sinode*));
		n = ss_iterof(ss_bufiterref, &i);
		n->used = sv_indexused(&n->i0);
		n->temperature = node->temperature;
		n->temperature_reads = node->temperature_reads;
		index->size += si_nodesize(n);
		si_nodelock(n);
		si_replace(index, node, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH|SI_TEMP, n);
		for (ss_iternext(ss_bufiterref, &i); ss_iterhas(ss_bufiterref, &i);
		     ss_iternext(ss_bufiterref, &i)) {
			n = ss_iterof(ss_bufiterref, &i);
			n->used = sv_indexused(&n->i0);
			n->temperature = node->temperature;
			n->temperature_reads = node->temperature_reads;
			index->size += si_nodesize(n);
			si_nodelock(n);
			si_insert(index, n);
			si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH|SI_TEMP, n);
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
		n  = ss_iterof(ss_bufiterref, &i);
		rc = si_nodeseal(n, r, index->scheme);
		if (ssunlikely(rc == -1)) {
			si_nodefree(node, r, 0);
			return -1;
		}
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
