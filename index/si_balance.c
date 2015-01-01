
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

static inline sibranch*
si_branchcreate(si *index, sr *r, sdc *c, sinode *parent, svindex *vindex, uint64_t vlsn)
{
	svmerge vmerge;
	sv_mergeinit(&vmerge);
	int rc = sv_mergeprepare(&vmerge, r, 1);
	if (srunlikely(rc == -1))
		return NULL;
	svmergesrc *s = sv_mergeadd(&vmerge, NULL);
	sr_iterinit(&s->src, &sv_indexiterraw, r);
	sr_iteropen(&s->src, vindex);
	sriter i;
	sr_iterinit(&i, &sv_mergeiter, r);
	sr_iteropen(&i, &vmerge, SR_GTE);

	/* merge iter is not used */
	sdmerge merge;
	sd_mergeinit(&merge, r, parent->self.id.id,
	             &i,
	             &c->build,
	             parent->file.size,
	             vindex->keymax,
	             vindex->used,
	             UINT64_MAX,
	             index->conf->node_page_size, 1,
	             vlsn);
	rc = sd_merge(&merge);
	if (srunlikely(rc == -1)) {
		sv_mergefree(&vmerge, r->a);
		sr_error(r->e, "%s", "memory allocation failed");
		goto error;
	}
	assert(rc == 1);
	sv_mergefree(&vmerge, r->a);

	sibranch *branch = si_branchnew(r);
	if (srunlikely(branch == NULL))
		goto error;
	sdid id = {
		.parent = parent->self.id.id,
		.flags  = SD_IDBRANCH,
		.id     = sr_seq(r->seq, SR_NSNNEXT)
	};
	rc = sd_mergecommit(&merge, &id);
	if (srunlikely(rc == -1))
		goto error;

	si_branchset(branch, &merge.index);
	rc = sd_buildwrite(&c->build, &branch->index, &parent->file);
	if (srunlikely(rc == -1)) {
		si_branchfree(branch, r);
		return NULL;
	}

	SR_INJECTION(r->i, SR_INJECTION_SI_BRANCH_0,
	             sr_error(r->e, "%s", "error injection");
	             si_branchfree(branch, r);
	             return NULL);

	if (index->conf->sync) {
		rc = si_nodesync(parent, r);
		if (srunlikely(rc == -1)) {
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
	if (srunlikely(n->used == 0)) {
		si_nodeunlock(n);
		si_unlock(index);
		return 0;
	}
	svindex *i;
	i = si_noderotate(n);
	si_unlock(index);

	sd_creset(c);
	sibranch *branch = si_branchcreate(index, r, c, n, i, vlsn);
	if (srunlikely(branch == NULL))
		return -1;

	/* commit */
	si_lock(index);
	branch->next = n->branch;
	n->branch = branch;
	n->branch_count++;
	uint32_t used = sv_indexused(i);
	n->used -= used;
	sr_quota(index->quota, SR_QREMOVE, used);
	svindex swap = *i;
	si_nodeunrotate(n);
	si_nodeunlock(n);
	si_plannerupdate(&index->p, SI_BRANCH|SI_COMPACT, n);
	si_unlock(index);

	/* gc */
	si_nodegc_index(r, &swap);
	return 1;
}

int si_compact(si *index, sr *r, sdc *c, siplan *plan, uint64_t vlsn)
{
	sinode *node = plan->node;
	assert(node->flags & SI_LOCK);

	/* read file */
	sd_creset(c);
	int rc = sr_bufensure(&c->c, r->a, node->file.size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	rc = sr_filepread(&node->file, 0, c->c.s, node->file.size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
		         node->file.file, strerror(errno));
		return -1;
	}
	sr_bufadvance(&c->c, node->file.size);

	/* prepare for compaction */
	svmerge merge;
	sv_mergeinit(&merge);
	rc = sv_mergeprepare(&merge, r, node->branch_count);
	if (srunlikely(rc == -1))
		return -1;
	uint32_t size_stream = 0;
	uint32_t size_key = 0;
	uint32_t gc = 0;
	sibranch *b = node->branch;
	while (b) {
		svmergesrc *s = sv_mergeadd(&merge, NULL);
		uint16_t key = sd_indexkeysize(&b->index);
		if (key > size_key)
			size_key = key;
		size_stream += sd_indextotal_kv(&b->index);
		sr_iterinit(&s->src, &sd_iter, r);
		sr_iteropen(&s->src, &b->index, c->c.s, 0);
		b = b->next;
	}
	sriter i;
	sr_iterinit(&i, &sv_mergeiter, r);
	sr_iteropen(&i, &merge, SR_GTE);
	rc = si_compaction(index, r, c, vlsn, node, &i, size_stream, size_key);
	if (srunlikely(rc == -1)) {
		sv_mergefree(&merge, r->a);
		return -1;
	}
	sv_mergefree(&merge, r->a);
	if (gc) {
		sr_quota(index->quota, SR_QREMOVE, gc);
	}
	return 0;
}
