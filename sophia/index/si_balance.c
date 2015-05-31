
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

static inline sibranch*
si_branchcreate(si *index, sr *r, sdc *c, sinode *parent, svindex *vindex, uint64_t vlsn)
{
	svmerge vmerge;
	sv_mergeinit(&vmerge);
	int rc = sv_mergeprepare(&vmerge, r, 1);
	if (ssunlikely(rc == -1))
		return NULL;
	svmergessc *s = sv_mergeadd(&vmerge, NULL);
	ss_iterinit(sv_indexiterraw, &s->ssc);
	ss_iteropen(sv_indexiterraw, &s->ssc, vindex);
	ssiter i;
	ss_iterinit(sv_mergeiter, &i);
	ss_iteropen(sv_mergeiter, &i, r, &vmerge, SS_GTE);

	/* merge iter is not used */
	sdmergeconf mergeconf = {
		.size_stream     = UINT32_MAX,
		.size_node       = UINT64_MAX,
		.size_page       = index->scheme->node_page_size,
		.checksum        = index->scheme->node_page_checksum,
		.compression     = index->scheme->compression,
		.compression_key = index->scheme->compression_key,
		.offset          = parent->file.size,
		.vlsn            = vlsn,
		.save_delete     = 1
	};
	sdmerge merge;
	sd_mergeinit(&merge, r, &i, &c->build, &mergeconf);
	rc = sd_merge(&merge);
	if (ssunlikely(rc == -1)) {
		sv_mergefree(&vmerge, r->a);
		sr_oom_malfunction(r->e);
		goto error;
	}
	assert(rc == 1);
	sv_mergefree(&vmerge, r->a);

	sibranch *branch = si_branchnew(r);
	if (ssunlikely(branch == NULL))
		goto error;
	sdid id = {
		.parent = parent->self.id.id,
		.flags  = SD_IDBRANCH,
		.id     = sr_seq(r->seq, SR_NSNNEXT)
	};
	rc = sd_mergecommit(&merge, &id);
	if (ssunlikely(rc == -1))
		goto error;

	si_branchset(branch, &merge.index);
	rc = sd_commit(&c->build, r, &branch->index, &parent->file);
	if (ssunlikely(rc == -1)) {
		si_branchfree(branch, r);
		return NULL;
	}

	SS_INJECTION(r->i, SS_INJECTION_SI_BRANCH_0,
	             sr_malfunction(r->e, "%s", "error injection");
	             si_branchfree(branch, r);
	             return NULL);

	if (index->scheme->sync) {
		rc = si_nodesync(parent, r);
		if (ssunlikely(rc == -1)) {
			si_branchfree(branch, r);
			return NULL;
		}
	}
	return branch;
error:
	sd_mergefree(&merge);
	return NULL;
}

int si_branch(si *index, sr *r, sdc *c, siplan *plan, uint64_t vlsn)
{
	sinode *n = plan->node;
	assert(n->flags & SI_LOCK);

	si_lock(index);
	if (ssunlikely(n->used == 0)) {
		si_nodeunlock(n);
		si_unlock(index);
		return 0;
	}
	svindex *i;
	i = si_noderotate(n);
	si_unlock(index);

	sd_creset(c);
	sibranch *branch = si_branchcreate(index, r, c, n, i, vlsn);
	if (ssunlikely(branch == NULL))
		return -1;

	/* commit */
	si_lock(index);
	branch->next = n->branch;
	n->branch = branch;
	n->branch_count++;
	uint32_t used = sv_indexused(i);
	n->used -= used;
	ss_quota(index->quota, SS_QREMOVE, used);
	svindex swap = *i;
	si_nodeunrotate(n);
	si_nodeunlock(n);
	si_plannerupdate(&index->p, SI_BRANCH|SI_COMPACT, n);
	si_unlock(index);

	/* gc */
	si_nodegc_index(r, &swap);
	return 1;
}

static inline int
si_noderead(sr *r, ssbuf *dest, sinode *node)
{
	int rc = ss_bufensure(dest, r->a, node->file.size);
	if (ssunlikely(rc == -1))
		return sr_oom_malfunction(r->e);
	rc = ss_filepread(&node->file, 0, dest->s, node->file.size);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' read error: %s",
		               node->file.file, strerror(errno));
		return -1;
	}
	ss_bufadvance(dest, node->file.size);
	return 0;
}

int si_compact(si *index, sr *r, sdc *c, siplan *plan, uint64_t vlsn)
{
	sinode *node = plan->node;
	assert(node->flags & SI_LOCK);

	/* read node file */
	sd_creset(c);
	int rc = si_noderead(r, &c->c, node);
	if (ssunlikely(rc == -1))
		return -1;

	/* prepare for compaction */
	rc = sd_censure(c, r, node->branch_count);
	if (ssunlikely(rc == -1))
		return sr_oom_malfunction(r->e);
	svmerge merge;
	sv_mergeinit(&merge);
	rc = sv_mergeprepare(&merge, r, node->branch_count);
	if (ssunlikely(rc == -1))
		return -1;
	uint32_t size_stream = 0;
	sdcbuf *cbuf = c->head;
	sibranch *b = node->branch;
	while (b) {
		svmergessc *s = sv_mergeadd(&merge, NULL);
		rc = ss_bufensure(&cbuf->b, r->a, b->index.h->sizevmax);
		if (ssunlikely(rc == -1))
			return sr_oom_malfunction(r->e);
		size_stream += sd_indextotal(&b->index);
		ss_iterinit(sd_iter, &s->ssc);
		ss_iteropen(sd_iter, &s->ssc, r, &b->index, c->c.s, 0,
		            index->scheme->compression, &cbuf->a, &cbuf->b);
		cbuf = cbuf->next;
		b = b->next;
	}
	ssiter i;
	ss_iterinit(sv_mergeiter, &i);
	ss_iteropen(sv_mergeiter, &i, r, &merge, SS_GTE);
	rc = si_compaction(index, r, c, vlsn, node, &i, size_stream);
	if (ssunlikely(rc == -1)) {
		sv_mergefree(&merge, r->a);
		return -1;
	}
	sv_mergefree(&merge, r->a);
	return 0;
}
