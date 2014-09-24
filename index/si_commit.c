
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>

int si_begin(sitx *t, sr *r, si *index, uint64_t lsvn, svlog *log, svv *v)
{
	t->index = index;
	t->lsvn  = lsvn;
	t->r     = r;
	t->log   = log;
	t->v     = v;
	si_lock(index);
	return 0;
}

int si_commit(sitx *t)
{
	si_unlock(t->index);
	return 0;
}

int si_rollback(sitx *t)
{
	(void)t;
	/* xxx */
	si_unlock(t->index);
	return 0;
}

static void
si_set(si *index, sr *r, uint64_t lsvn, svv *v)
{
	uint32_t size = sv_vsize(v);
	/* ensure memory limit */
	si_qos(index, 0, size);
	/* match node */
	sriter i;
	sr_iterinit(&i, &si_iter, r);
	sr_iteropen(&i, index, SR_ROUTE, sv_vkey(v), v->keysize);
	sinode *node = sr_iterof(&i);
	assert(node != NULL);
	/* update node */
	svindex *vindex = si_nodeindex(node);
	svv *prev = NULL;
	sv_indexset(vindex, r, lsvn, v, &prev);
	node->icount++;
	node->iused += size;
	if (srunlikely(prev)) {
		uint32_t size_prev = sv_vsize(prev);
		node->iused -= size_prev;
		si_qos(index, 1, size_prev);
	}
	/* schedule node */
	si_plan(&index->plan, SI_BRANCH, node);
	if (srunlikely(prev)) {
		sl *log = (sl*)prev->log;
		if (log) {
			sr_gcsweep(&log->gc, 1);
		}
		sv_vfree(r->a, prev);
	}
}

int si_write(sitx *t)
{
	si_set(t->index, t->r, t->lsvn, t->v);
	return 0;
}

int si_writelog(sitx *t)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, t->r);
	sr_iteropen(&i, &t->log->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		svv *v = vp->v;
		si_set(t->index, t->r, t->lsvn, v);
	}
	return 0;
}
