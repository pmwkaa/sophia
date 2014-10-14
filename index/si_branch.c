
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

int si_branch(si *index, sr *r, sdc *c, uint64_t lsvn, uint32_t wm)
{
	si_lock(index);
	uint32_t limit = wm;
	if (si_qoslimit(index))
		limit = 0;
	sinode *n = si_planpeek(&index->plan, SI_BRANCH, limit);
	if (srunlikely(n == NULL)) {
		si_unlock(index);
		return 0;
	}
	/*si_planprint_branch(&index->plan);*/
	uint32_t iused   = n->iused;
	uint32_t iusedkv = n->iusedkv;
	uint32_t icount  = n->icount;
	if (srunlikely(iused == 0)) {
		n->flags &= ~SI_BRANCH;
		si_unlock(index);
		return 0;
	}
	svindex *i;
	i = si_noderotate(n);
	si_unlock(index);

	sd_creset(c);
	srbuf *result = &c->a;
	
	/* dump index */
	sriter indexi;
	sr_iterinit(&indexi, &sv_indexiterraw, r);
	sr_iteropen(&indexi, i);
	sisplit s = {
		.parent      = n,
		.flags       = SD_IDBRANCH,
		.i           = &indexi,
		.size_key    = i->keymax,
		.size_stream = iusedkv,
		.size_node   = UINT32_MAX,
		.lsvn        = lsvn,
		.conf        = index->conf
	};
	int rc = si_split(&s, r, c, result);
	if (srunlikely(rc == -1))
		return -1;
	assert(sr_bufused(result) == sizeof(sinode*));

	SR_INJECTION(r->i, SR_INJECTION_SI_BRANCH_0,
	             sr_error(r->e, "%s", "error injection");
	             si_splitfree(result, r);
	             return -1);

	/* sync and rename */
	sinode *q = *(sinode**)result->s;
	if (index->conf->sync) {
		rc = si_nodesync(q, r);
		if (srunlikely(rc == -1)) {
			si_splitfree(result, r);
			return -1;
		}
	}
	rc = si_nodecomplete(q, r, index->conf);
	if (srunlikely(rc == -1)) {
		si_splitfree(result, r);
		return -1;
	}

	SR_INJECTION(r->i, SR_INJECTION_SI_BRANCH_1,
	             si_splitfree(result, r);
	             sr_error(r->e, "%s", "error injection");
	             return -1);

	/* commit */
	svindex swap = *i;

	si_lock(index);
	n->flags &= ~SI_BRANCH;
	q->next = n->next;
	n->next = q;
	si_nodeunrotate(n);
	n->lv++;
	n->iused   -= iused;
	n->iusedkv -= iusedkv;
	n->icount  -= icount;
	si_plan(&index->plan, SI_BRANCH|SI_MERGE, n);
	si_qos(index, 1, iused);
	si_unlock(index);

	/* gc */
	sr_iterinit(&indexi, &sv_indexiterraw, r);
	sr_iteropen(&indexi, &swap);
	for (; sr_iterhas(&indexi); sr_iternext(&indexi)) {
		sv *v = sr_iterof(&indexi);
		svv *vv = v->v;
		if (vv->log) {
			sr_gcsweep(&((sl*)vv->log)->gc, 1);
		}
	}
	sv_indexfree(&swap, r);
	return 1;
}
