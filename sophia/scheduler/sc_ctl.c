
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
#include <libsl.h>
#include <libsi.h>
#include <libsy.h>
#include <libsc.h>

int sc_ctl_call(sc *s, uint64_t vlsn)
{
	int rc = sr_statusactive(s->r->status);
	if (ssunlikely(rc == 0))
		return 0;
	scworker *w = sc_workerpool_pop(&s->wp, s->r);
	if (ssunlikely(w == NULL))
		return -1;
	rc = sc_step(s, w, vlsn);
	sc_workerpool_push(&s->wp, w);
	return rc;
}

int sc_ctl_branch(sc *s, uint64_t vlsn, si *index)
{
	sr *r = s->r;
	int rc = sr_statusactive(r->status);
	if (ssunlikely(rc == 0))
		return 0;
	srzone *z = sr_zoneof(r);
	scworker *w = sc_workerpool_pop(&s->wp, r);
	if (ssunlikely(w == NULL))
		return -1;
	while (1) {
		uint64_t vlsn_lru = si_lru_vlsn(index);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_BRANCH,
			.a         = z->branch_wm,
			.b         = 0,
			.c         = 0,
			.node      = NULL
		};
		rc = si_plan(index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(index, &w->dc, &plan, vlsn, vlsn_lru);
		if (ssunlikely(rc == -1))
			break;
	}
	sc_workerpool_push(&s->wp, w);
	return rc;
}

int sc_ctl_compact(sc *s, uint64_t vlsn, si *index)
{
	sr *r = s->r;
	int rc = sr_statusactive(r->status);
	if (ssunlikely(rc == 0))
		return 0;
	srzone *z = sr_zoneof(r);
	scworker *w = sc_workerpool_pop(&s->wp, r);
	if (ssunlikely(w == NULL))
		return -1;
	while (1) {
		uint64_t vlsn_lru = si_lru_vlsn(index);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_COMPACT,
			.a         = z->compact_wm,
			.b         = z->compact_mode,
			.c         = 0,
			.node      = NULL
		};
		rc = si_plan(index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(index, &w->dc, &plan, vlsn, vlsn_lru);
		if (ssunlikely(rc == -1))
			break;
	}
	sc_workerpool_push(&s->wp, w);
	return rc;
}

int sc_ctl_compact_index(sc *s, uint64_t vlsn, si *index)
{
	sr *r = s->r;
	int rc = sr_statusactive(r->status);
	if (ssunlikely(rc == 0))
		return 0;
	srzone *z = sr_zoneof(r);
	scworker *w = sc_workerpool_pop(&s->wp, r);
	if (ssunlikely(w == NULL))
		return -1;
	while (1) {
		uint64_t vlsn_lru = si_lru_vlsn(index);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_COMPACT_INDEX,
			.a         = z->branch_wm,
			.b         = 0,
			.c         = 0,
			.node      = NULL
		};
		rc = si_plan(index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(index, &w->dc, &plan, vlsn, vlsn_lru);
		if (ssunlikely(rc == -1))
			break;
	}
	sc_workerpool_push(&s->wp, w);
	return rc;
}

int sc_ctl_anticache(sc *s)
{
	uint64_t asn = sr_seq(s->r->seq, SR_ASNNEXT);
	ss_mutexlock(&s->lock);
	s->anticache_asn = asn;
	s->anticache_storage = s->anticache_limit;
	s->anticache = 1;
	sc_start(s, SI_ANTICACHE);
	ss_mutexunlock(&s->lock);
	return 0;
}

int sc_ctl_snapshot(sc *s)
{
	uint64_t ssn = sr_seq(s->r->seq, SR_SSNNEXT);
	ss_mutexlock(&s->lock);
	s->snapshot_ssn = ssn;
	s->snapshot = 1;
	sc_start(s, SI_SNAPSHOT);
	ss_mutexunlock(&s->lock);
	return 0;
}

int sc_ctl_checkpoint(sc *s)
{
	uint64_t lsn = sr_seq(s->r->seq, SR_LSN);
	ss_mutexlock(&s->lock);
	s->checkpoint_lsn = lsn;
	s->checkpoint = 1;
	sc_start(s, SI_CHECKPOINT);
	ss_mutexunlock(&s->lock);
	return 0;
}

int sc_ctl_gc(sc *s)
{
	ss_mutexlock(&s->lock);
	s->gc = 1;
	sc_start(s, SI_GC);
	ss_mutexunlock(&s->lock);
	return 0;
}

int sc_ctl_lru(sc *s)
{
	ss_mutexlock(&s->lock);
	s->lru = 1;
	sc_start(s, SI_LRU);
	ss_mutexunlock(&s->lock);
	return 0;
}

int sc_ctl_backup(sc *s)
{
	return sc_backupstart(s);
}

int sc_ctl_backup_event(sc *s)
{
	int event = 0;
	ss_mutexlock(&s->lock);
	if (ssunlikely(s->backup_events > 0)) {
		s->backup_events--;
		event = 1;
	}
	ss_mutexunlock(&s->lock);
	return event;
}

int sc_ctl_shutdown(sc *s, si *i)
{
	ss_mutexlock(&s->lock);
	s->shutdown_pending++;
	ss_listappend(&s->shutdown, &i->link);
	ss_mutexunlock(&s->lock);
	return 0;
}
