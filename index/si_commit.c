
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

void
si_vgc(sra *a, svv *gc)
{
	svv *v = gc;
	while (v) {
		svv *n = v->next;
		sl *log = (sl*)v->log;
		if (log) {
			sr_gcsweep(&log->gc, 1);
		}
		sr_free(a, v);
		v = n;
	}
}

void si_begin(sitx *t, sr *r, si *index, uint64_t lsvn, svlog *log)
{
	t->index = index;
	t->lsvn  = lsvn;
	t->r     = r;
	t->log   = log;
	si_lock(index);
}

void si_commit(sitx *t) {
	si_unlock(t->index);
}

void si_rollback(sitx *t) {
	si_unlock(t->index);
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
	svv *vgc = NULL;
	sv_indexset(vindex, r, lsvn, v, &vgc);
	node->icount++;
	node->iused += size;
	node->iusedkv += v->keysize + v->valuesize;
	if (srunlikely(vgc)) {
		uint32_t size_vgc = sv_vsize(vgc);
		node->iused -= size_vgc;
		node->iusedkv -= vgc->keysize + vgc->valuesize;
		si_qos(index, 1, size_vgc);
	}
	/* schedule node */
	si_plan(&index->plan, SI_BRANCH, node);
	if (srunlikely(vgc))
		si_vgc(r->a, vgc);
}

void si_write(sitx *t, int check)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, t->r);
	sr_iteropen(&i, &t->log->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		svv *v = vp->v;
		if (check && si_querycommited(t->index, t->r, vp)) {
			si_vgc(t->r->a, v);
			continue;
		}
		si_set(t->index, t->r, t->lsvn, v);
	}
}
