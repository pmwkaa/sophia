
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

static inline int
si_compactadd(svmerge *m, sr *r, sinode *n,
              uint32_t *size_stream,
              uint32_t *size_key)
{
	svmergesrc *src = sv_mergeadd(m);
	assert(src != NULL);
	uint16_t key = sd_indexkeysize(&n->index);
	if (key > *size_key)
		*size_key = key;
	*size_stream += sd_indextotal_kv(&n->index);
	sr_iterinit(&src->i, &sd_iter, r);
	sr_iteropen(&src->i, &n->map, 0);
	return 0;
}

int si_compact(si *index, sr *r, sdc *c, siplan *plan, uint64_t vlsn)
{
	sinode *node = plan->node;
	sd_creset(c);
	svmerge merge;
	sv_mergeinit(&merge);
	int rc = sv_mergeprepare(&merge, r, node->lv + 1, 0);
	if (srunlikely(rc == -1))
		return -1;
	uint32_t size_stream = 0;
	uint32_t size_key = 0;
	sinode *n;
	for (n = node->next; n; n = n->next)
		si_compactadd(&merge, r, n, &size_stream, &size_key);
	si_compactadd(&merge, r, node, &size_stream, &size_key);
	sriter i;
	sr_iterinit(&i, &sv_mergeiter, r);
	sr_iteropen(&i, &merge, SR_GTE);
	rc = si_compaction(index, r, c, vlsn, node, &i, size_stream, size_key);
	if (srunlikely(rc == -1)) {
		sv_mergefree(&merge, r->a);
		return -1;
	}
	sv_mergefree(&merge, r->a);
	return 0;
}
