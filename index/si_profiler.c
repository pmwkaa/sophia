
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

int si_profiler(siprofiler *p, sr *r)
{
	sr_seqlock(r->seq);
	p->seq = *r->seq;
	sr_sequnlock(r->seq);
	srrbnode *pn;
	sinode *n;
	pn = sr_rbmin(&p->i->i);
	while (pn) {
		n = srcast(pn, sinode, node);
		p->total_node_size += n->file.size;
		p->total_node_count++;
		p->count += n->icount;
		p->count += n->index.h->keys;
		p->total_branch_count += n->lv;
		if (p->total_branch_max < n->lv)
			p->total_branch_max = n->lv;
		n = n->next;
		while (n) {
			p->count += n->index.h->keys;
			p->total_branch_size += n->file.size;
			n = n->next;
		}
		pn = sr_rbnext(&p->i->i, pn);
	}
	p->memory_used = p->i->qos_used;
	return 0;
}
