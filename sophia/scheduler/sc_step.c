
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

static inline int
sc_rotate(sc*s, scworker *w)
{
	ss_trace(&w->trace, "%s", "log rotation");
	int rc = sl_poolrotate_ready(s->lp);
	if (rc) {
		rc = sl_poolrotate(s->lp);
		if (ssunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static inline int
sc_gc(sc *s, scworker *w)
{
	ss_trace(&w->trace, "%s", "log gc");
	int rc = sl_poolgc(s->lp);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static inline int
sc_execute(sctask *t, scworker *w, uint64_t vlsn)
{
	si *index;
	if (ssunlikely(t->shutdown))
		index = t->shutdown;
	else
		index = t->db->index;

	si_plannertrace(&t->plan, index->scheme->id, &w->trace);
	uint64_t vlsn_lru = si_lru_vlsn(index);
	int rcret = 0;
	int rc = si_execute(index, &w->dc, &t->plan, vlsn, vlsn_lru);
	if (ssunlikely(rc == -1))
		rcret = -1;

	if (t->shutdown) {
		rc = so_destroy(index->object, 0);
		if (ssunlikely(rc == -1))
			rcret = -1;
	}
	return rcret;
}

static int
sc_plan(sc *s, siplan *plan,
        uint32_t quota, uint32_t quota_limit,
        scdb **dbret)
{
	if (s->rr >= s->count)
		s->rr = 0;
	int start = s->rr;
	int limit = s->count;
	int i = start;
	int rc_inprogress = 0;
	int rc;
first_half:
	while (i < limit) {
		scdb *db = s->i[i];
		if (ssunlikely(! si_active(db->index))) {
			i++;
			continue;
		}
		if (quota != SC_QNONE) {
			if (db->workers[quota] >= quota_limit) {
				rc_inprogress = 2;
				i++;
				continue;
			}
		}
		rc = si_plan(db->index, plan);
		switch (rc) {
		case 1:
			*dbret = db;
			s->rr = i + 1;
			return 1;
		case 2: rc_inprogress = 2;
		case 0: break;
		}
		i++;
	}
	if (i > start) {
		i = 0;
		limit = start;
		goto first_half;
	}
	s->rr = 0;
	return rc_inprogress;
}

static int
sc_do(sc *s, sctask *task, scworker *w, uint64_t vlsn)
{
	ss_trace(&w->trace, "%s", "schedule");
	uint64_t now = ss_utime();

	srzone *zone = sr_zoneof(s->r);
	scdb *sdb = NULL;

	si_planinit(&task->plan);

	task->checkpoint_complete = 0;
	task->anticache_complete = 0;
	task->snapshot_complete = 0;
	task->backup_complete = 0;
	task->rotate = 0;
	task->read = 0;
	task->gc = 0;
	task->db = NULL;
	task->shutdown = NULL;

	ss_mutexlock(&s->lock);

	/* asynchronous reader */
	if (s->read == 0) {
		switch (zone->async) {
		case 2:
			if (sc_readpool_queue(&s->rp) == 0)
				break;
		case 1:
			s->read = 1;
			task->read = zone->async;
			ss_mutexunlock(&s->lock);
			return 0;
		}
	}

	/* log gc and rotation */
	if (s->rotate == 0)
	{
		task->rotate = 1;
		s->rotate = 1;
	}

	/* checkpoint */
	int in_progress = 0;
	int rc;
checkpoint:
	if (s->checkpoint) {
		task->plan.plan = SI_CHECKPOINT;
		task->plan.a = s->checkpoint_lsn;
		rc = sc_plan(s, &task->plan, SC_QNONE, 0, &sdb);
		switch (rc) {
		case 1:
			sdb->workers[SC_QBRANCH]++;
			si_ref(sdb->index, SI_REFBE);
			task->db = sdb;
			task->gc = 1;
			ss_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			in_progress = 1;
			break;
		case 0: /* complete checkpoint */
			s->checkpoint = 0;
			s->checkpoint_lsn_last = s->checkpoint_lsn;
			s->checkpoint_lsn = 0;
			task->checkpoint_complete = 1;
			break;
		}
	}

	/* apply zone policy */
	switch (zone->mode) {
	case 0:  /* compact_index */
		break;
	case 1:  /* compact_index + branch_count prio */
		assert(0);
		break;
	case 2:  /* checkpoint */
	{
		if (in_progress) {
			ss_mutexunlock(&s->lock);
			goto no_job;
		}
		uint64_t lsn = sr_seq(s->r->seq, SR_LSN);
		s->checkpoint_lsn = lsn;
		s->checkpoint = 1;
		goto checkpoint;
	}
	default: /* branch + compact */
		assert(zone->mode == 3);
	}

	/* database shutdown-drop */
	if (s->shutdown.n > 0) {
		sslist *p, *n;
		ss_listforeach_safe(&s->shutdown.list, p, n) {
			so *o = sscast(p, so, link);
			si *index = sscast(o, si, link);
			task->plan.plan = SI_SHUTDOWN;
			rc = si_plan(index, &task->plan);
			if (rc == 1) {
				so_listdel(&s->shutdown, &index->link);
				sc_del(s, index->object, 0);
				task->shutdown = index;
				task->gc = 1;
				ss_mutexunlock(&s->lock);
				return 1;
			}
		}
	}

	/* anti-cache */
	if (s->anticache) {
		task->plan.plan = SI_ANTICACHE;
		task->plan.a = s->anticache_asn;
		task->plan.b = s->anticache_storage;
		rc = sc_plan(s, &task->plan, SC_QNONE, 0, &sdb);
		switch (rc) {
		case 1:
			si_ref(sdb->index, SI_REFBE);
			task->db = sdb;
			uint64_t size = task->plan.c;
			if (size > 0) {
				if (ssunlikely(size > s->anticache_storage))
					s->anticache_storage = 0;
				else
					s->anticache_storage -= size;
			}
			ss_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			in_progress = 1;
			break;
		case 0: /* complete */
			s->anticache = 0;
			s->anticache_asn_last = s->anticache_asn;
			s->anticache_asn = 0;
			s->anticache_storage = 0;
			s->anticache_time = now;
			task->anticache_complete = 1;
			break;
		}
	} else {
		if (zone->anticache_period) {
			if ( (now - s->anticache_time) >= ((uint64_t)zone->anticache_period * 1000000) ) {
				s->anticache = 1;
				s->anticache_storage = s->anticache_limit;
				s->anticache_asn = sr_seq(s->r->seq, SR_ASNNEXT);
			}
		}
	}

	/* snapshot */
	if (s->snapshot) {
		task->plan.plan = SI_SNAPSHOT;
		task->plan.a = s->snapshot_ssn;
		rc = sc_plan(s, &task->plan, SC_QNONE, 0, &sdb);
		switch (rc) {
		case 1:
			si_ref(sdb->index, SI_REFBE);
			task->db = sdb;
			ss_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			in_progress = 1;
			break;
		case 0: /* complete */
			s->snapshot = 0;
			s->snapshot_ssn_last = s->snapshot_ssn;
			s->snapshot_ssn = 0;
			s->snapshot_time = now;
			task->snapshot_complete = 1;
			break;
		}
	} else {
		if (zone->snapshot_period) {
			if ( (now - s->snapshot_time) >= ((uint64_t)zone->snapshot_period * 1000000) ) {
				s->snapshot = 1;
				s->snapshot_ssn = sr_seq(s->r->seq, SR_SSNNEXT);
			}
		}
	}

	/* backup */
	if (s->backup)
	{
		/* backup procedure.
		 *
		 * state 0 (start)
		 * -------
		 *
		 * a. disable log gc
		 * b. mark to start backup (state 1)
		 *
		 * state 1 (background, delayed start)
		 * -------
		 *
		 * a. create backup_path/<bsn.incomplete> directory
		 * b. create database directories
		 * c. create log directory
		 * d. state 2
		 *
		 * state 2 (background, copy)
		 * -------
		 *
		 * a. schedule and execute node backup which bsn < backup_bsn
		 * b. state 3
		 *
		 * state 3 (background, completion)
		 * -------
		 *
		 * a. rotate log file
		 * b. copy log files
		 * c. enable log gc, schedule gc
		 * d. rename <bsn.incomplete> into <bsn>
		 * e. set last backup, set COMPLETE
		 *
		*/
		if (s->backup == 1) {
			/* state 1 */
			rc = sc_backupbegin(s);
			if (ssunlikely(rc == -1)) {
				sc_backuperror(s);
				goto backup_error;
			}
			s->backup = 2;
		}
		/* state 2 */
		task->plan.plan = SI_BACKUP;
		task->plan.a = s->backup_bsn;
		rc = sc_plan(s, &task->plan, SC_QBACKUP, zone->backup_prio, &sdb);
		switch (rc) {
		case 1:
			sdb->workers[SC_QBACKUP]++;
			si_ref(sdb->index, SI_REFBE);
			task->db = sdb;
			ss_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			break;
		case 0: /* state 3 */
			rc = sc_backupend(s, w);
			if (ssunlikely(rc == -1)) {
				sc_backuperror(s);
				goto backup_error;
			}
			s->backup_events++;
			task->gc = 1;
			task->backup_complete = 1;
			break;
		}
backup_error:;
	}

	/* garbage-collection */
	if (s->gc) {
		task->plan.plan = SI_GC;
		task->plan.a = vlsn;
		task->plan.b = zone->gc_wm;
		rc = sc_plan(s, &task->plan, SC_QGC, zone->gc_prio, &sdb);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			si_ref(sdb->index, SI_REFBE);
			sdb->workers[SC_QGC]++;
			task->db = sdb;
			ss_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			break;
		case 0: /* state 3 */
			s->gc = 0;
			s->gc_time = now;
			break;
		}
	} else {
		if (zone->gc_prio && zone->gc_period) {
			if ( (now - s->gc_time) >= ((uint64_t)zone->gc_period * 1000000) ) {
				s->gc = 1;
			}
		}
	}

	/* lru */
	if (s->lru) {
		task->plan.plan = SI_LRU;
		rc = sc_plan(s, &task->plan, SC_QLRU, zone->lru_prio, &sdb);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			si_ref(sdb->index, SI_REFBE);
			sdb->workers[SC_QLRU]++;
			task->db = sdb;
			ss_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			break;
		case 0: /* state 3 */
			s->lru = 0;
			s->lru_time = now;
			break;
		}
	} else {
		if (zone->lru_prio && zone->lru_period) {
			if ( (now - s->lru_time) >= ((uint64_t)zone->lru_period * 1000000) ) {
				s->lru = 1;
			}
		}
	}

	/* index aging */
	if (s->age) {
		task->plan.plan = SI_AGE;
		task->plan.a = zone->branch_age * 1000000; /* ms */
		task->plan.b = zone->branch_age_wm;
		rc = sc_plan(s, &task->plan, SC_QBRANCH, zone->branch_prio, &sdb);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			si_ref(sdb->index, SI_REFBE);
			sdb->workers[SC_QBRANCH]++;
			task->db = sdb;
			ss_mutexunlock(&s->lock);
			return 1;
		case 0:
			s->age = 0;
			s->age_time = now;
			break;
		}
	} else {
		if (zone->branch_prio && zone->branch_age_period) {
			if ( (now - s->age_time) >= ((uint64_t)zone->branch_age_period * 1000000) ) {
				s->age = 1;
			}
		}
	}

	/* compact_index (compaction with in-memory index) */
	if (zone->mode == 0) {
		task->plan.plan = SI_COMPACT_INDEX;
		task->plan.a = zone->branch_wm;
		rc = sc_plan(s, &task->plan, SC_QNONE, 0, &sdb);
		if (rc == 1) {
			si_ref(sdb->index, SI_REFBE);
			task->db = sdb;
			task->gc = 1;
			ss_mutexunlock(&s->lock);
			return 1;
		}
		ss_mutexunlock(&s->lock);
		goto no_job;
	}

	/* branching */

	/* schedule branch task using following
	 * priority:
	 *
	 * a. peek node with the largest in-memory index
	 *    which is equal or greater then branch
	 *    watermark.
	 *    If nothing is found, stick to b.
	 *
	 * b. peek node with the largest in-memory index,
	 *    which has oldest update time.
	 *
	 * c. if no branch work is needed, schedule a
	 *    compaction job
	 *
	 */
	task->plan.plan = SI_BRANCH;
	task->plan.a = zone->branch_wm;
	rc = sc_plan(s, &task->plan, SC_QBRANCH, zone->branch_prio, &sdb);
	if (rc == 1) {
		sdb->workers[SC_QBRANCH]++;
		si_ref(sdb->index, SI_REFBE);
		task->db = sdb;
		task->gc = 1;
		ss_mutexunlock(&s->lock);
		return 1;
	}

	/* compaction */
	task->plan.plan = SI_COMPACT;
	task->plan.a = zone->compact_wm;
	task->plan.b = zone->compact_mode;
	rc = sc_plan(s, &task->plan, SC_QNONE, 0, &sdb);
	if (rc == 1) {
		si_ref(sdb->index, SI_REFBE);
		task->db = sdb;
		ss_mutexunlock(&s->lock);
		return 1;
	}

	ss_mutexunlock(&s->lock);
no_job:
	si_planinit(&task->plan);
	return 0;
}

static inline int
sc_complete(sc *s, sctask *t)
{
	ss_mutexlock(&s->lock);
	scdb *sdb = t->db;
	switch (t->plan.plan) {
	case SI_BRANCH:
	case SI_AGE:
	case SI_CHECKPOINT:
		sdb->workers[SC_QBRANCH]--;
		break;
	case SI_COMPACT_INDEX:
		break;
	case SI_BACKUP:
	case SI_BACKUPEND:
		sdb->workers[SC_QBACKUP]--;
		break;
	case SI_SNAPSHOT:
		break;
	case SI_ANTICACHE:
		break;
	case SI_GC:
		sdb->workers[SC_QGC]--;
		break;
	case SI_LRU:
		sdb->workers[SC_QLRU]--;
		break;
	}
	if (sdb && sdb->db)
		si_unref(sdb->index, SI_REFBE);
	if (t->rotate == 1)
		s->rotate = 0;
	if (t->read)
		s->read = 0;
	ss_mutexunlock(&s->lock);
	return 0;
}

static int
sc_reader(sc *s, scworker *w, sctask *t)
{
	ss_trace(&w->trace, "%s", "reader");
	int block = t->read == 1;
	do {
		int rc = sr_statusactive(s->r->status);
		if (ssunlikely(rc == 0))
			break;
		scread *r = (scread*)sc_readpool_pop(&s->rp, block);
		if (ssunlikely(r == NULL))
			continue;
		sc_read(r, s);
		sc_readpool_ready(&s->rp, &r->o);
		/* trigger ready event */
		ss_triggerrun(s->on_event);
	} while (block);
	return 0;
}

int sc_step(sc *s, scworker *w, uint64_t vlsn)
{
	sctask task;
	int rc = sc_do(s, &task, w, vlsn);
	int rc_job = rc;
	if (task.rotate) {
		rc = sc_rotate(s, w);
		if (ssunlikely(rc == -1))
			goto error;
	}
	if (task.read) {
		rc = sc_reader(s, w, &task);
		if (ssunlikely(rc == -1)) {
			goto error;
		}
	}
	/* trigger backup competion */
	if (task.backup_complete)
		ss_triggerrun(s->on_event);
	if (rc_job > 0) {
		rc = sc_execute(&task, w, vlsn);
		if (ssunlikely(rc == -1)) {
			if (task.plan.plan != SI_BACKUP &&
			    task.plan.plan != SI_BACKUPEND) {
				sr_statusset(&task.db->index->status,
				             SR_MALFUNCTION);
				goto error;
			}
			ss_mutexlock(&s->lock);
			sc_backuperror(s);
			ss_mutexunlock(&s->lock);
		}
	}
	if (task.gc) {
		rc = sc_gc(s, w);
		if (ssunlikely(rc == -1))
			goto error;
	}
	sc_complete(s, &task);
	ss_trace(&w->trace, "%s", "sleep");
	return rc_job;
error:
	ss_trace(&w->trace, "%s", "malfunction");
	return -1;
}
