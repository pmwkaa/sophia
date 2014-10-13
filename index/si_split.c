
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

int si_splitfree(srbuf *result, sr *r)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, result, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sinode *p = sr_iterof(&i);
		si_nodefree(p, r);
	}
	return 0;
}

int si_split(sisplit *s, sr *r, sdc *c, srbuf *result)
{
	int count = 0;
	int rc;
	sdmerge merge;
	sd_mergeinit(&merge, r, s->parent->id.id, s->flags,
	             s->i, &c->build,
	             s->size_key,
	             s->size_stream,
	             s->size_node,
	             s->conf->node_page_size, s->lsvn);
	while ((rc = sd_merge(&merge)) > 0)
	{
		sinode *n = si_nodenew(r);
		if (srunlikely(n == NULL))
			goto error;
		sdid id = {
			.parent = s->parent->id.id,
			.flags  = s->flags,
			.id     = sr_seq(r->seq, SR_NSNNEXT)
		};
		rc = sd_mergecommit(&merge, &id);
		if (srunlikely(rc == -1))
			goto error;
		if (s->flags & SD_IDBRANCH)
			rc = si_nodecreate(n, r, s->conf, &id, &merge.index, &c->build);
		else
			rc = si_nodecreate_attach(n, r, s->conf, &id, &merge.index, &c->build);
		if (srunlikely(rc == -1))
			goto error;
		rc = sr_bufadd(result, r->a, &n, sizeof(sinode*));
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "%s", "memory allocation failed");
			si_nodefree(n, r);
			goto error;
		}
		sd_buildreset(&c->build);
		count++;
	}
	if (srunlikely(rc == -1))
		goto error;

	return 0;
error:
	si_splitfree(result, r);
	sd_mergefree(&merge);
	return -1;
}
