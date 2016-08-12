
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
	svmerge merge;
	sv_mergeinit(&merge);
	rc = sv_mergeprepare(&merge, r, 1 + 1);
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

	sdcbuf *cbuf = &c->e;
	s = sv_mergeadd(&merge, NULL);
	sdreadarg arg = {
		.from_compaction     = 1,
		.io                  = &c->io,
		.index               = &node->index,
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
	rc = ss_iteropen(sd_read, &s->src, &arg, NULL);
	if (ssunlikely(rc == -1))
		return -1;
	size_stream += sd_indextotal(&node->index);
	count += sd_indexkeys(&node->index);

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
	svindex *vindex;
	vindex = si_noderotate(node);
	si_unlock(index);

	uint64_t size_stream = vindex->used;
	ssiter i;
	ss_iterinit(sv_indexiter, &i);
	ss_iteropen(sv_indexiter, &i, &index->r, vindex, SS_GTE, NULL);
	return si_compact(index, c, plan, vlsn, &i, size_stream);
}
