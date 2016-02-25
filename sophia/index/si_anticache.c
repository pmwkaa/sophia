
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

int si_anticache(si *index, siplan *plan)
{
	sinode *n = plan->node;
	sr *r = &index->r;

	/* promote */
	if (n->flags & SI_PROMOTE) {
		sibranch *b = n->branch;
		while (b) {
			int rc;
			rc = si_branchload(b, r, &n->file);
			if (ssunlikely(rc == -1))
				return -1;
			b = b->next;
		}
		si_lock(index);
		n->flags &= ~SI_PROMOTE;
		n->in_memory = 1;
		si_nodeunlock(n);
		si_unlock(index);
		return 0;
	}

	/* revoke */
	assert(n->flags & SI_REVOKE);
	si_lock(index);
	n->flags &= ~SI_REVOKE;
	n->in_memory = 0;
	si_unlock(index);
	sibranch *b = n->branch;
	while (b) {
		ss_blobfree(&b->copy);
		ss_blobinit(&b->copy, r->vfs);
		b = b->next;
	}
	si_lock(index);
	si_nodeunlock(n);
	si_unlock(index);
	return 0;
}
