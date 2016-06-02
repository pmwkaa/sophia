
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

int sd_mergeinit(sdmerge *m, sr *r, ssiter *i, sdbuild *build, ssqf *qf,
                 svupsert *upsert, sdmergeconf *conf)
{
	m->conf      = conf;
	m->build     = build;
	m->qf        = qf;
	m->r         = r;
	m->merge     = i;
	m->processed = 0;
	m->current   = 0;
	m->limit     = 0;
	m->resume    = 0;
	if (conf->amqf) {
		int rc = ss_qfensure(qf, r->a, conf->stream);
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
	}
	sd_indexinit(&m->index);
	ss_iterinit(sv_writeiter, &m->i);
	ss_iteropen(sv_writeiter, &m->i, r, i, upsert,
	            (uint64_t)conf->size_page, sizeof(sdv),
	            conf->expire,
	            conf->timestamp,
	            conf->vlsn,
	            conf->vlsn_lru,
	            conf->save_delete,
	            conf->save_upsert);
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
	if (conf->amqf)
		ss_qfreset(m->qf);
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
	                   conf->expire,
	                   conf->compression_copy,
	                   conf->compression,
	                   conf->compression_if);
	if (ssunlikely(rc == -1))
		return -1;
	while (ss_iterhas(sv_writeiter, &m->i))
	{
		sv *v = ss_iterof(sv_writeiter, &m->i);
		uint8_t flags = sv_flags(v);
		if (sv_writeiter_is_duplicate(&m->i))
			flags |= SVDUP;
		rc = sd_buildadd(m->build, m->r, v, flags);
		if (ssunlikely(rc == -1))
			return -1;
		if (conf->amqf) {
			ss_qfadd(m->qf, sv_hash(v, m->r));
		}
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
	ssqf *qf = NULL;
	if (m->conf->amqf)
		qf = m->qf;
	return sd_indexcommit(&m->index, m->r, id, qf, offset);
}
