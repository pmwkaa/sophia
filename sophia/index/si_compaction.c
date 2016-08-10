
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

static inline int
si_branchcreate(si *index, sdc *c, sinode *parent, svindex *vindex, uint64_t vlsn,
                sibranch **result)
{
	sr *r = &index->r;
	sibranch *branch = NULL;

	/* prepare direct_io stream */
	int rc;
	if (index->scheme.direct_io) {
		rc = sd_ioprepare(&c->io, r,
		                  index->scheme.direct_io,
		                  index->scheme.direct_io_page_size,
		                  index->scheme.direct_io_buffer_size);
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
	}

	svmerge vmerge;
	sv_mergeinit(&vmerge);
	rc = sv_mergeprepare(&vmerge, r, 1);
	if (ssunlikely(rc == -1))
		return -1;
	svmergesrc *s = sv_mergeadd(&vmerge, NULL);
	ss_iterinit(sv_indexiter, &s->src);
	ss_iteropen(sv_indexiter, &s->src, r, vindex, SS_GTE, NULL);
	ssiter i;
	ss_iterinit(sv_mergeiter, &i);
	ss_iteropen(sv_mergeiter, &i, r, &vmerge, SS_GTE);

	/* merge iter is not used */
	uint32_t timestamp = ss_timestamp();
	sdmergeconf mergeconf = {
		.stream              = vindex->count,
		.size_stream         = UINT32_MAX,
		.size_node           = UINT64_MAX,
		.size_page           = index->scheme.node_page_size,
		.checksum            = index->scheme.node_page_checksum,
		.expire              = index->scheme.expire,
		.timestamp           = timestamp,
		.compression         = index->scheme.compression,
		.compression_if      = index->scheme.compression_if,
		.direct_io           = index->scheme.direct_io,
		.direct_io_page_size = index->scheme.direct_io_page_size,
		.vlsn                = vlsn,
		.save_delete         = 1,
		.save_upsert         = 1
	};
	sdmerge merge;
	rc = sd_mergeinit(&merge, r, &i, &c->build, &c->build_index,
	                  &c->upsert, &mergeconf);
	if (ssunlikely(rc == -1))
		return -1;

	sdid id = {
		.parent = parent->self.id.id,
		.flags  = SD_IDBRANCH,
		.id     = sr_seq(r->seq, SR_NSNNEXT)
	};
	rc = si_noderename_inprogress(parent, r, &index->scheme, &id);
	if (ssunlikely(rc == -1))
		goto e0;

	while ((rc = sd_merge(&merge)) > 0)
	{
		assert(branch == NULL);

		/* write pages */
		uint64_t start = parent->file.size;
		uint64_t offset = start;
		while ((rc = sd_mergepage(&merge, offset)) == 1)
		{
			rc = sd_writepage(r, &parent->file, &c->io, merge.build);
			if (ssunlikely(rc == -1))
				goto e0;
			offset = sd_iosize(&c->io, &parent->file);
		}
		if (ssunlikely(rc == -1))
			goto e0;

		offset = sd_iosize(&c->io, &parent->file);
		rc = sd_mergeend(&merge, &id, offset);
		if (ssunlikely(rc == -1))
			goto e0;

		/* write index */
		rc = sd_writeindex(r, &parent->file, &c->io, &merge.index);
		if (ssunlikely(rc == -1))
			goto e0;

		if (index->scheme.sync) {
			rc = ss_filesync_range(&parent->file, start, parent->file.size);
			if (ssunlikely(rc == -1)) {
				sr_malfunction(r->e, "db file '%s' sync error: %s",
				               ss_pathof(&parent->file.path),
				               strerror(errno));
				goto e0;
			}
		}

		SS_INJECTION(r->i, SS_INJECTION_SI_BRANCH_0,
		             sd_mergefree(&merge);
		             sr_malfunction(r->e, "%s", "error injection");
		             return -1);

		rc = si_noderename_complete(parent, r, &index->scheme);
		if (ssunlikely(rc == -1))
			goto e0;

		/* create new branch object */
		branch = si_branchnew(r);
		if (ssunlikely(branch == NULL))
			goto e0;
		si_branchset(branch, &merge.index);
	}
	sv_mergefree(&vmerge, r->a);

	if (ssunlikely(rc == -1)) {
		sr_oom_malfunction(r->e);
		goto e0;
	}

	/* in case of expire, branch may not be created if there
	 * are no keys left */
	if (ssunlikely(branch == NULL))
		return 0;

	/* mmap support */
	if (index->scheme.mmap) {
		ss_mmapinit(&parent->map_swap);
		rc = ss_vfsmmap(r->vfs, &parent->map_swap, parent->file.fd,
		                parent->file.size, 1);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "db file '%s' mmap error: %s",
			               ss_pathof(&parent->file.path),
			               strerror(errno));
			goto e1;
		}
	}

	*result = branch;
	return 0;
e0:
	sd_mergefree(&merge);
	sv_mergefree(&vmerge, r->a);
	return -1;
e1:
	si_branchfree(branch, r);
	return -1;
}

int si_branch(si *index, sdc *c, siplan *plan, uint64_t vlsn)
{
	sr *r = &index->r;
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

	sibranch *branch = NULL;
	int rc = si_branchcreate(index, c, n, i, vlsn, &branch);
	if (ssunlikely(rc == -1))
		return -1;
	if (ssunlikely(branch == NULL)) {
		si_lock(index);
		uint32_t used = i->used;
		n->used -= used;
		svindex swap = *i;
		si_nodeunrotate(n);
		si_nodeunlock(n);
		si_plannerupdate(&index->p, SI_BRANCH|SI_COMPACT, n);
		si_unlock(index);
		si_nodegc_index(r, &swap);
		return 0;
	}

	/* commit */
	si_lock(index);
	branch->next = n->branch;
	n->branch->link = branch;
	n->branch = branch;
	n->branch_count++;
	uint32_t used = i->used;
	n->used -= used;
	index->size +=
		sd_indexsize_ext(branch->index.h) +
		sd_indextotal(&branch->index);
	svindex swap = *i;
	si_nodeunrotate(n);
	si_nodeunlock(n);
	si_plannerupdate(&index->p, SI_BRANCH|SI_COMPACT, n);
	ssmmap swap_map = n->map;
	n->map = n->map_swap;
	memset(&n->map_swap, 0, sizeof(n->map_swap));
	si_unlock(index);

	/* gc */
	if (index->scheme.mmap) {
		int rc = ss_vfsmunmap(r->vfs, &swap_map);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "db file '%s' munmap error: %s",
			               ss_pathof(&n->file.path),
			               strerror(errno));
			return -1;
		}
	}
	si_nodegc_index(r, &swap);
	return 1;
}

int si_compact(si *index, sdc *c, siplan *plan,
               uint64_t vlsn,
               ssiter *vindex,
               uint64_t vindex_used)
{
	sr *r = &index->r;
	sinode *node = plan->node;
	assert(node->flags & SI_LOCK);

	/* prepare direct_io stream */
	int rc;
	if (index->scheme.direct_io) {
		rc = sd_ioprepare(&c->io, r,
		                  index->scheme.direct_io,
		                  index->scheme.direct_io_page_size,
		                  index->scheme.direct_io_buffer_size);
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
	}

	/* prepare for compaction */
	rc = sd_censure(c, r, node->branch_count);
	if (ssunlikely(rc == -1))
		return sr_oom_malfunction(r->e);
	svmerge merge;
	sv_mergeinit(&merge);
	rc = sv_mergeprepare(&merge, r, node->branch_count + 1);
	if (ssunlikely(rc == -1))
		return -1;

	int use_mmap = index->scheme.mmap;
	ssmmap *map = &node->map;

	/* include vindex into merge process */
	svmergesrc *s;
	uint32_t count = 0;
	uint64_t size_stream = 0;
	if (vindex) {
		s = sv_mergeadd(&merge, vindex);
		size_stream = vindex_used;
	}

	sdcbuf *cbuf = c->head;
	sibranch *b = node->branch;
	while (b) {
		s = sv_mergeadd(&merge, NULL);
		/* choose compression type */
		sdreadarg arg = {
			.from_compaction     = 1,
			.io                  = &c->io,
			.index               = &b->index,
			.buf                 = &cbuf->a,
			.buf_read            = &c->d,
			.index_iter          = &cbuf->index_iter,
			.page_iter           = &cbuf->page_iter,
			.use_mmap            = use_mmap,
			.use_mmap_copy       = 0,
			.use_compression     = index->scheme.compression,
			.use_direct_io       = index->scheme.direct_io,
			.direct_io_page_size = index->scheme.direct_io_page_size,
			.compression_if      = index->scheme.compression_if,
			.has                 = 0,
			.has_vlsn            = 0,
			.o                   = SS_GTE,
			.mmap                = map,
			.file                = &node->file,
			.r                   = r
		};
		ss_iterinit(sd_read, &s->src);
		int rc = ss_iteropen(sd_read, &s->src, &arg, NULL);
		if (ssunlikely(rc == -1))
			return -1;
		size_stream += sd_indextotal(&b->index);
		count += sd_indexkeys(&b->index);
		cbuf = cbuf->next;
		b = b->next;
	}
	ssiter i;
	ss_iterinit(sv_mergeiter, &i);
	ss_iteropen(sv_mergeiter, &i, r, &merge, SS_GTE);
	rc = si_merge(index, c, node, vlsn, &i, size_stream, count);
	sv_mergefree(&merge, r->a);
	return rc;
}

int si_compact_index(si *index, sdc *c, siplan *plan, uint64_t vlsn)
{
	sinode *node = plan->node;

	si_lock(index);
	if (ssunlikely(node->used == 0)) {
		si_nodeunlock(node);
		si_unlock(index);
		return 0;
	}
	svindex *vindex;
	vindex = si_noderotate(node);
	si_unlock(index);

	uint64_t size_stream = vindex->used;
	ssiter i;
	ss_iterinit(sv_indexiter, &i);
	ss_iteropen(sv_indexiter, &i, &index->r, vindex, SS_GTE, NULL);
	return si_compact(index, c, plan, vlsn, &i, size_stream);
}
