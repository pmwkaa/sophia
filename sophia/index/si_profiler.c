
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

int si_profilerbegin(siprofiler *p, si *i)
{
	memset(p, 0, sizeof(*p));
	p->i = i;
	si_lock(i);
	return 0;
}

int si_profilerend(siprofiler *p)
{
	si_unlock(p->i);
	return 0;
}

int si_profiler(siprofiler *p)
{
	uint64_t memory_used = 0;
	ssrbnode *pn;
	sinode *n;
	pn = ss_rbmin(&p->i->i);
	while (pn) {
		n = sscast(pn, sinode, node);
		p->total_node_count++;
		p->count += n->i0.count;
		p->count += n->i1.count;
		memory_used += n->i0.used;
		memory_used += n->i1.used;

		sibranch *b = &n->self;
		p->count += b->index.h->keys;
		p->count_dup += b->index.h->dupkeys;
		int indexsize = sd_indexsize_ext(b->index.h);
		p->total_node_size += indexsize + b->index.h->total;
		p->total_node_origin_size += indexsize + b->index.h->totalorigin;
		p->total_page_count += b->index.h->count;

		pn = ss_rbnext(&p->i->i, pn);
	}
	p->memory_used = memory_used;
	p->read_disk  = p->i->read_disk;
	p->read_cache = p->i->read_cache;
	return 0;
}
