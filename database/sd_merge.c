
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

int sd_mergeinit(sdmerge *m, sr *r, sriter *i,
                 sdbuild *build,
                 uint32_t size_key,
                 uint32_t size_stream,
                 uint32_t size_node,
                 uint32_t size_page, uint64_t lsvn)
{
	m->r = r;
	m->build = build;
	sd_indexinit(&m->index);
	m->size_key    = size_key;
	m->size_stream = size_stream;
	m->size_node   = size_node;
	m->size_page   = size_page;
	m->merged      = 0;
	sr_iterinit(&m->i, &sv_seaveiter, r);
	sr_iteropen(&m->i, i, (uint64_t)size_page, sizeof(sdv), lsvn);
	return 0;
}

int sd_mergefree(sdmerge *m)
{
	sd_indexfree(&m->index, m->r->a);
	return 0;
}

int sd_merge(sdmerge *m)
{
	if (srunlikely(! sr_iterhas(&m->i)))
		return 0;
	sd_buildreset(m->build);

	sd_indexinit(&m->index);
	int rc = sd_indexbegin(&m->index, m->r->a, m->size_key);
	if (srunlikely(rc == -1))
		return -1;

	uint32_t left = (m->size_stream - m->merged);
	uint32_t limit;
	if (left >= (m->size_node * 2))
		limit = m->size_node;
	else
	if (left > (m->size_node))
		limit = m->size_node / 2;
	else
		limit = left;

	while (sr_iterhas(&m->i) && (sd_buildsize(m->build) < limit))
	{
		rc = sd_buildbegin(m->build, m->size_key);
		if (srunlikely(rc == -1))
			return -1;
		while (sr_iterhas(&m->i)) {
			sv *v = sr_iterof(&m->i);
			rc = sd_buildadd(m->build, v);
			if (srunlikely(rc == -1))
				return -1;
			sr_iternext(&m->i);
		}
		rc = sd_buildend(m->build);
		if (srunlikely(rc == -1))
			return -1;
		rc = sd_indexadd(&m->index, m->r->a,
		                 sd_buildoffset(m->build) + sizeof(srversion),
		                 sd_buildheader(m->build)->size + sizeof(sdpageheader),
		                 sd_buildheader(m->build)->count,
		                 sd_buildmin(m->build)->key,
		                 sd_buildmin(m->build)->keysize,
		                 sd_buildmax(m->build)->key,
		                 sd_buildmax(m->build)->keysize,
		                 sd_buildheader(m->build)->lsnmin,
		                 sd_buildheader(m->build)->lsnmax);
		if (srunlikely(rc == -1))
			return -1;
		sd_buildcommit(m->build);
		if (srunlikely(! sv_seaveiter_resume(&m->i)))
			break;
	}
	rc = sd_indexcommit(&m->index, m->r->a);
	if (srunlikely(rc == -1))
		return -1;

	m->merged += sd_buildsize(m->build);
	return 1;
}
