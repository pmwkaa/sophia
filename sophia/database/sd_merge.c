
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

int sd_mergeinit(sdmerge *m, sr *r, ssiter *i, sdbuild *build,
                 svupdate *update, sdmergeconf *conf)
{
	m->conf      = conf;
	m->build     = build;
	m->r         = r;
	m->merge     = i;
	m->processed = 0;
	m->current   = 0;
	m->limit     = 0;
	m->resume    = 0;
	sd_indexinit(&m->index);
	ss_iterinit(sv_writeiter, &m->i);
	ss_iteropen(sv_writeiter, &m->i, r, i, update,
	            (uint64_t)conf->size_page, sizeof(sdv),
	            conf->vlsn,
	            conf->save_delete,
	            conf->save_update);
	return 0;
}

int sd_mergefree(sdmerge *m)
{
	sd_indexfree(&m->index, m->r);
	return 0;
}

static inline int
sd_mergehas(sdmerge *m)
{
	if (! ss_iterhas(sv_writeiter, &m->i))
		return 0;
	if (m->current > m->limit)
		return 0;
	return 1;
}

int sd_merge(sdmerge *m)
{
	if (ssunlikely(! ss_iterhas(sv_writeiter, &m->i)))
		return 0;
	sdmergeconf *conf = m->conf;
	sd_indexinit(&m->index);
	int rc = sd_indexbegin(&m->index, m->r);
	if (ssunlikely(rc == -1))
		return -1;
	m->current = 0;
	m->limit   = 0;
	uint64_t processed = m->processed;
	uint64_t left = (conf->size_stream - processed);
	if (left >= (conf->size_node * 2)) {
		m->limit = conf->size_node;
	} else
	if (left > (conf->size_node)) {
		m->limit = conf->size_node * 2;
	} else {
		m->limit = UINT64_MAX;
	}
	return sd_mergehas(m);
}

int sd_mergepage(sdmerge *m, uint64_t offset)
{
	sdmergeconf *conf = m->conf;
	sd_buildreset(m->build, m->r);

	if (m->resume) {
		m->resume = 0;
		if (ssunlikely(! sv_writeiter_resume(&m->i)))
			return 0;
	}
	if (! sd_mergehas(m))
		return 0;
	int rc;
	rc = sd_buildbegin(m->build, m->r, conf->checksum,
	                   conf->compression_key,
	                   conf->compression,
	                   conf->compression_if);
	if (ssunlikely(rc == -1))
		return -1;
	while (ss_iterhas(sv_writeiter, &m->i))
	{
		sv *v = ss_iterof(sv_writeiter, &m->i);
		uint8_t flags = 0;
		if (sv_writeiter_is_duplicate(&m->i))
			flags = SVDUP;
		rc = sd_buildadd(m->build, m->r, v, flags);
		if (ssunlikely(rc == -1))
			return -1;
		ss_iternext(sv_writeiter, &m->i);
	}
	rc = sd_buildend(m->build, m->r);
	if (ssunlikely(rc == -1))
		return -1;
	rc = sd_indexadd(&m->index, m->r, m->build, offset);
	if (ssunlikely(rc == -1))
		return -1;

	m->current = sd_indextotal(&m->index);
	m->resume  = 1;
	return 1;
}

int sd_mergecommit(sdmerge *m, sdid *id, uint64_t offset)
{
	m->processed += sd_indextotal(&m->index);
	return sd_indexcommit(&m->index, m->r, id, offset);
}
