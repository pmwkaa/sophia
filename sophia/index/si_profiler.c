
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
	p->temperature_min = 100;
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
		size += snprintf(p->histogram_branch_sz + size,
		                 sizeof(p->histogram_branch_sz) - size,
		                 "[%d]:%d ", i,
		                 p->histogram_branch[i]);
		i++;
	}
	if (p->histogram_branch_20plus) {
		size += snprintf(p->histogram_branch_sz + size,
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

static void
si_profiler_histogram_temperature(siprofiler *p)
{
	/* build histogram */
	static struct {
		int nodes;
		int branches;
	} h[101];
	memset(h, 0, sizeof(h));
	sinode *n;
	ssrqnode *pn = NULL;
	while ((pn = ss_rqprev(&p->i->p.temp, pn)))
	{
		n = sscast(pn, sinode, nodetemp);
		h[pn->v].nodes++;
		h[pn->v].branches += n->branch_count;
	}

	/* prepare histogram string */
	int count = 0;
	int i = 100;
	int size = 0;
	while (i >= 0 && count < 10) {
		if (h[i].nodes == 0) {
			i--;
			continue;
		}
		size += snprintf(p->histogram_temperature_sz + size,
		                 sizeof(p->histogram_temperature_sz) - size,
		                 "[%d]:%d-%d ", i,
		                 h[i].nodes, h[i].branches);
		i--;
		count++;
	}
	if (size == 0)
		p->histogram_temperature_ptr = NULL;
	else {
		p->histogram_temperature_ptr = p->histogram_temperature_sz;
	}
}

int si_profiler(siprofiler *p)
{
	uint32_t temperature_total = 0;
	uint64_t memory_used = 0;
	ssrbnode *pn;
	sinode *n;
	pn = ss_rbmin(&p->i->i);
	while (pn) {
		n = sscast(pn, sinode, node);
		if (p->temperature_max < n->temperature)
			p->temperature_max = n->temperature;
		if (p->temperature_min > n->temperature)
			p->temperature_min = n->temperature;
		temperature_total += n->temperature;
		p->total_node_count++;
		p->count += n->i0.count;
		p->count += n->i1.count;
		p->total_branch_count += n->branch_count;
		if (p->total_branch_max < n->branch_count)
			p->total_branch_max = n->branch_count;
		if (n->branch_count < 20)
			p->histogram_branch[n->branch_count]++;
		else
			p->histogram_branch_20plus++;
		memory_used += n->i0.used;
		memory_used += n->i1.used;
		sibranch *b = n->branch;
		while (b) {
			p->count += b->index.h->keys;
			p->count_dup += b->index.h->dupkeys;
			int indexsize = sd_indexsize_ext(b->index.h);
			p->total_snapshot_size += indexsize;
			p->total_node_size += indexsize + b->index.h->total;
			p->total_node_origin_size += indexsize + b->index.h->totalorigin;
			p->total_page_count += b->index.h->count;
			if (b->index.h->extensions & SD_INDEXEXT_AMQF) {
				p->total_amqf_size +=
					sizeof(sdindexamqf) + sd_indexamqf(&b->index)->size;
			}
			b = b->next;
		}
		pn = ss_rbnext(&p->i->i, pn);
	}
	if (p->total_node_count > 0) {
		p->total_branch_avg =
			p->total_branch_count / p->total_node_count;
		p->temperature_avg =
			temperature_total / p->total_node_count;
	}
	p->memory_used = memory_used;
	p->read_disk  = p->i->read_disk;
	p->read_cache = p->i->read_cache;

	si_profiler_histogram_branch(p);
	si_profiler_histogram_temperature(p);
	return 0;
}
