
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
#include <libsd.h>

int sd_mergeinit(sdmerge *m, sr *r, ssiter *i, sdbuild *build, sdmergeconf *conf)
{
	m->conf      = conf;
	m->build     = build;
	m->r         = r;
	m->merge     = i;
	m->processed = 0;
	sd_indexinit(&m->index);
	ss_iterinit(sv_writeiter, &m->i);
	ss_iteropen(sv_writeiter, &m->i, i, (uint64_t)conf->size_page, sizeof(sdv),
	            conf->vlsn,
	            conf->save_delete);
	return 0;
}

int sd_mergefree(sdmerge *m)
{
	sd_indexfree(&m->index, m->r);
	return 0;
}

int sd_merge(sdmerge *m)
{
	if (ssunlikely(! ss_iterhas(sv_writeiter, &m->i)))
		return 0;
	sdmergeconf *conf = m->conf;
	sd_buildreset(m->build);

	sd_indexinit(&m->index);
	int rc = sd_indexbegin(&m->index, m->r, conf->offset);
	if (ssunlikely(rc == -1))
		return -1;

	uint64_t processed = m->processed;
	uint64_t current = 0;
	uint64_t left = (conf->size_stream - processed);
	uint64_t limit;
	if (left >= (conf->size_node * 2)) {
		limit = conf->size_node;
	} else
	if (left > (conf->size_node)) {
		limit = conf->size_node * 2;
	} else {
		limit = UINT64_MAX;
	}

	while (ss_iterhas(sv_writeiter, &m->i) && (current <= limit))
	{
		rc = sd_buildbegin(m->build, m->r, conf->checksum,
		                   conf->compression,
		                   conf->compression_key);
		if (ssunlikely(rc == -1))
			return -1;
		while (ss_iterhas(sv_writeiter, &m->i)) {
			sv *v = ss_iterof(sv_writeiter, &m->i);
			uint8_t flags = sv_mergeisdup(m->merge);
			rc = sd_buildadd(m->build, m->r, v, flags);
			if (ssunlikely(rc == -1))
				return -1;
			ss_iternext(sv_writeiter, &m->i);
		}
		rc = sd_buildend(m->build, m->r);
		if (ssunlikely(rc == -1))
			return -1;
		rc = sd_indexadd(&m->index, m->r, m->build);
		if (ssunlikely(rc == -1))
			return -1;
		sd_buildcommit(m->build, m->r);

		current = sd_indextotal(&m->index);
		if (ssunlikely(! sv_writeiter_resume(&m->i)))
			break;
	}

	m->processed += sd_indextotal(&m->index);
	return 1;
}

int sd_mergecommit(sdmerge *m, sdid *id)
{
	return sd_indexcommit(&m->index, m->r, id);
}
