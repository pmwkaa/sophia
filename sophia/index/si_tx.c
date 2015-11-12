
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
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>

void si_begin(sitx *x, si *index, int ro)
{
	x->ro = ro;
	x->index = index;
	ss_listinit(&x->nodelist);
	si_lock(index);
}

static inline void
si_temperature_set(si *i, sinode *n)
{
	uint64_t total = i->read_disk + i->read_cache;
	if (ssunlikely(total == 0))
		return;
	n->temperature = (n->temperature_reads * 100ULL) / total;
}

void si_commit(sitx *x)
{
	/* reschedule nodes */
	sslist *i, *n;
	ss_listforeach_safe(&x->nodelist, i, n) {
		sinode *node = sscast(i, sinode, commit);
		ss_listinit(&node->commit);
		if (x->ro) {
			si_temperature_set(x->index, node);
			si_plannerupdate(&x->index->p, SI_TEMP, node);
		} else {
			si_plannerupdate(&x->index->p, SI_BRANCH, node);
		}
	}
	si_unlock(x->index);
}
