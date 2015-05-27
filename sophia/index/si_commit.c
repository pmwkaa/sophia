
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

uint32_t
si_vgc(ssa *a, svv *gc)
{
	uint32_t used = 0;
	svv *v = gc;
	while (v) {
		used += sv_vsize(v);
		svv *n = v->next;
		sl *log = (sl*)v->log;
		if (log)
			ss_gcsweep(&log->gc, 1);
		ss_free(a, v);
		v = n;
	}
	return used;
}

void si_begin(sitx *t, sr *r, si *index, uint64_t vlsn, uint64_t time,
              svlog *l,
              svlogindex *li)
{
	t->index = index;
	t->time  = time;
	t->vlsn  = vlsn;
	t->r     = r;
	t->l     = l;
	t->li    = li;
	ss_listinit(&t->nodelist);
	si_lock(index);
}

void si_commit(sitx *t)
{
	/* reschedule nodes */
	sslist *i, *n;
	ss_listforeach_safe(&t->nodelist, i, n) {
		sinode *node = sscast(i, sinode, commit);
		ss_listinit(&node->commit);
		si_plannerupdate(&t->index->p, SI_BRANCH, node);
	}
	si_unlock(t->index);
}

static inline void
si_set(sitx *t, svv *v)
{
	si *index = t->index;
	t->index->update_time = t->time;
	/* match node */
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, t->r, index, SS_ROUTE, sv_vpointer(v), v->size);
	sinode *node = ss_iterof(si_iter, &i);
	assert(node != NULL);
	/* update node */
	svindex *vindex = si_nodeindex(node);
	svv *vgc = NULL;
	sv_indexset(vindex, t->r, t->vlsn, v, &vgc);
	node->update_time = index->update_time;
	node->used += sv_vsize(v);
	if (ssunlikely(vgc)) {
		uint32_t gc = si_vgc(t->r->a, vgc);
		node->used -= gc;
		ss_quota(index->quota, SS_QREMOVE, gc);
	}
	if (ss_listempty(&node->commit))
		ss_listappend(&t->nodelist, &node->commit);
}

void si_write(sitx *t, int check)
{
	svlogv *cv = sv_logat(t->l, t->li->head);
	int c = t->li->count;
	while (c) {
		svv *v = cv->v.v;
		if (check && si_querycommited(t->index, t->r, &cv->v)) {
			uint32_t gc = si_vgc(t->r->a, v);
			ss_quota(t->index->quota, SS_QREMOVE, gc);
			goto next;
		}
		si_set(t, v);
next:
		cv = sv_logat(t->l, cv->next);
		c--;
	}
}
