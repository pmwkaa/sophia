
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

static inline int si_set(sitx *x, svv *v, uint64_t time)
{
	si *index = x->index;
	index->update_time = time;
	/* match node */
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, index->r, index, SS_GTE,
	            sv_vpointer(v), v->size);
	sinode *node = ss_iterof(si_iter, &i);
	assert(node != NULL);
	/* insert into node index */
	svindex *vindex = si_nodeindex(node);
	svindexpos pos;
	sv_indexget(vindex, index->r, &pos, v);
	sv_indexupdate(vindex, &pos, v);
	/* update node */
	node->update_time = index->update_time;
	node->used += sv_vsize(v);
	if (index->scheme->lru)
		si_lru_add(index, v);
	si_txtrack(x, node);
	return 0;
}

void si_write(sitx *x, int check, uint64_t time, svlog *l, svlogindex *li)
{
	sr *r = x->index->r;
	svlogv *cv = sv_logat(l, li->head);
	int c = li->count;
	while (c) {
		svv *v = cv->v.v;
		if (check && si_readcommited(x->index, r, &cv->v)) {
			uint32_t gc = si_gcv(r, v);
			ss_quota(r->quota, SS_QREMOVE, gc);
			goto next;
		}
		if (v->flags & SVGET) {
			sv_vfree(r, v);
			goto next;
		}
		si_set(x, v, time);
next:
		cv = sv_logat(l, cv->next);
		c--;
	}
	return;
}
