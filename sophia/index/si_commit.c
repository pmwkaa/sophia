
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

void si_begin(sitx *t, si *index, uint64_t vlsn, uint64_t time,
              svlog *l,
              svlogindex *li)
{
	t->index = index;
	t->time  = time;
	t->vlsn  = vlsn;
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

static inline svv*
si_update(sitx *t, svv *head, svv *v)
{
	if (sslikely((v->flags & SVUPDATE) == 0))
		return v;
	if (sslikely(head == NULL))
		return v;
	if ((head->flags & SVUPDATE) > 0)
		return v;
	sr *r = t->index->r;
	sv a, b, c;
	sv_init(&a, &sv_vif, head, NULL);
	sv_init(&b, &sv_vif, v, NULL);
	int rc = sv_update(r, &a, &b, &c);
	if (ssunlikely(rc == -1))
		return NULL;
	((svv*)c.v)->log = v->log;
	/* gc */
	uint32_t grow = sv_vsize(c.v);
	uint32_t gc = sv_vsize(v);
	ss_free(r->a, v);
	ss_quota(r->quota, SS_QGROW, grow);
	ss_quota(r->quota, SS_QREMOVE, gc);
	return c.v;
}

static inline int
si_set(sitx *t, svv *v)
{
	si *index = t->index;
	t->index->update_time = t->time;
	/* match node */
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, t->index->r, index, SS_ROUTE,
	            sv_vpointer(v), v->size);
	sinode *node = ss_iterof(si_iter, &i);
	assert(node != NULL);
	/* insert into node index */
	svindex *vindex = si_nodeindex(node);
	svindexpos pos;
	svv *head = sv_indexget(vindex, t->index->r, &pos, v);
	/* apply update */
	v = si_update(t, head, v);
	if (ssunlikely(v == NULL))
		return -1;
	sv_indexupdate(vindex, &pos, v);
	/* update node */
	node->update_time = index->update_time;
	node->used += sv_vsize(v);
	if (ss_listempty(&node->commit))
		ss_listappend(&t->nodelist, &node->commit);
	return 0;
}

void si_write(sitx *t, int check)
{
	svlogv *cv = sv_logat(t->l, t->li->head);
	int c = t->li->count;
	while (c) {
		svv *v = cv->v.v;
		if (check && si_querycommited(t->index, t->index->r, &cv->v)) {
			uint32_t gc = si_gcv(t->index->r->a, v);
			ss_quota(t->index->r->quota, SS_QREMOVE, gc);
			goto next;
		}
		si_set(t, v);
next:
		cv = sv_logat(t->l, cv->next);
		c--;
	}
	return;
}
