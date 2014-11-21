
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

static void
si_profiler_histogram_branch(siprofiler *p)
{
	/* prepare histogram string */
	int size = 0;
	int i = 0;
	while (i < 20) {
		if (p->histogram_branch[i] == 0) {
			i++;
			continue;
		}
		size += snprintf(p->histogram_branch_sz,
		                 sizeof(p->histogram_branch_sz) - size,
		                 "[%d]:%d ", i,
		                 p->histogram_branch[i]);
		i++;
	}
	if (p->histogram_branch_20plus) {
		size += snprintf(p->histogram_branch_sz,
		                 sizeof(p->histogram_branch_sz) - size,
		                 "[20+]:%d ",
		                 p->histogram_branch_20plus);
	}
	if (size == 0)
		p->histogram_branch_ptr = NULL;
	else {
		p->histogram_branch_ptr = p->histogram_branch_sz;
	}
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
		p->count += n->i0.count;
		p->count += n->i1.count;
		p->count += n->index.h->keys;
		p->total_branch_count += n->lv;
		if (p->total_branch_max < n->lv)
			p->total_branch_max = n->lv;
		if (n->lv < 20)
			p->histogram_branch[n->lv]++;
		else
			p->histogram_branch_20plus++;
		n = n->next;
		while (n) {
			p->count += n->index.h->keys;
			p->total_branch_size += n->file.size;
			n = n->next;
		}
		pn = sr_rbnext(&p->i->i, pn);
	}
	if (p->total_node_count > 0)
		p->total_branch_avg =
			p->total_branch_count / p->total_node_count;
	p->memory_used = p->i->qos_used;

	si_profiler_histogram_branch(p);
	return 0;
}
