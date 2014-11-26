
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

int si_branch(si *index, sr *r, sdc *c, siplan *plan, uint64_t vlsn)
{
	si_lock(index);
	sinode *n = plan->node;
	if (srunlikely(n->used == 0)) {
		si_nodeunlock(n);
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
		.size_stream = i->used,
		.size_node   = UINT32_MAX,
		.vlsn        = vlsn,
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
	q->next = n->next;
	n->next = q;
	n->lv++;
	uint32_t used = sv_indexused(i);
	n->used -= used;
	index->used -= used;
	si_nodeunrotate(n);
	si_nodeunlock(n);
	si_plannerupdate(&index->p, SI_BRANCH|SI_COMPACT, n);
	si_unlock(index);

	sr_quota(index->quota, SR_QREMOVE, used);

	/* gc */
	si_nodegc_index(r, &swap);
	return 1;
}
