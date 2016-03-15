
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

	si_plannertrace(&t->plan, index->scheme.id, &w->trace);
	uint64_t vlsn_lru = si_lru_vlsn(index);
	return si_execute(index, &w->dc, &t->plan, vlsn, vlsn_lru);
}

static inline scdb*
sc_peek(sc *s)
{
	if (s->rr >= s->count)
		s->rr = 0;
	int start = s->rr;
	int limit = s->count;
	int i = start;
first_half:
	while (i < limit) {
		scdb *db = s->i[i];
		if (ssunlikely(! si_active(db->index))) {
			i++;
			continue;
		}
		s->rr = i;
		return db;
	}
	if (i > start) {
		i = 0;
		limit = start;
		goto first_half;
	}
	s->rr = 0;
	return NULL;
}

static inline void
sc_next(sc *s)
{
	s->rr++;
	if (s->rr >= s->count)
		s->rr = 0;
}

static inline int
sc_plan(sc *s, siplan *plan)
{
	scdb *db = s->i[s->rr];
	return si_plan(db->index, plan);


}

static inline int
sc_planquota(sc *s, siplan *plan, uint32_t quota, uint32_t quota_limit)
{
	scdb *db = s->i[s->rr];
	if (db->workers[quota] >= quota_limit)
		return 2;
	return si_plan(db->index, plan);
}

static inline int
sc_do_shutdown(sc *s, sctask *task)
{
	if (sslikely(s->shutdown_pending == 0))
		return 0;
	sslist *p, *n;
	ss_listforeach_safe(&s->shutdown, p, n) {
		si *index = sscast(p, si, link);
		task->plan.plan = SI_SHUTDOWN;
		int rc;
		rc = si_plan(index, &task->plan);
		if (rc == 1) {
			s->shutdown_pending--;
			ss_listunlink(&index->link);
			sc_del(s, index, 0);
			task->shutdown = index;
			task->db = NULL;
			task->gc = 1;
			return 1;
		}
	}
	return 0;
}

static int
sc_do(sc *s, sctask *task, scworker *w, srzone *zone,
      scdb *db, uint64_t vlsn, uint64_t now)
{
	ss_trace(&w->trace, "%s", "schedule");

	/* checkpoint */
	int rc;
	if (s->checkpoint) {
		task->plan.plan = SI_CHECKPOINT;
		task->plan.a = s->checkpoint_lsn;
		rc = sc_plan(s, &task->plan);
		switch (rc) {
		case 1:
			db->workers[SC_QBRANCH]++;
			si_ref(db->index, SI_REFBE);
			task->db = db;
			task->gc = 1;
			return 1;
		case 0: /* complete */
			if (sc_end(s, db, SI_CHECKPOINT)) {
				s->checkpoint = 0;
				s->checkpoint_lsn_last = s->checkpoint_lsn;
				s->checkpoint_lsn = 0;
			}
			break;
		}
	}

	/* anti-cache */
	if (s->anticache) {
		task->plan.plan = SI_ANTICACHE;
		task->plan.a = s->anticache_asn;
		task->plan.b = s->anticache_storage;
		rc = sc_plan(s, &task->plan);
		switch (rc) {
		case 1:
			si_ref(db->index, SI_REFBE);
			task->db = db;
			uint64_t size = task->plan.c;
			if (size > 0) {
				if (ssunlikely(size > s->anticache_storage))
					s->anticache_storage = 0;
				else
					s->anticache_storage -= size;
			}
			return 1;
		case 0: /* complete */
			if (sc_end(s, db, SI_ANTICACHE)) {
				s->anticache = 0;
				s->anticache_asn_last = s->anticache_asn;
				s->anticache_asn = 0;
				s->anticache_storage = 0;
				s->anticache_time = now;
			}
			break;
		}
	}

	/* snapshot */
	if (s->snapshot) {
		task->plan.plan = SI_SNAPSHOT;
		task->plan.a = s->snapshot_ssn;
		rc = sc_plan(s, &task->plan);
		switch (rc) {
		case 1:
			si_ref(db->index, SI_REFBE);
			task->db = db;
			return 1;
		case 0: /* complete */
			if (sc_end(s, db, SI_SNAPSHOT)) {
				s->snapshot = 0;
				s->snapshot_ssn_last = s->snapshot_ssn;
				s->snapshot_ssn = 0;
				s->snapshot_time = now;
			}
			break;
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
		rc = sc_planquota(s, &task->plan, SC_QBACKUP, zone->backup_prio);
		switch (rc) {
		case 1:
			db->workers[SC_QBACKUP]++;
			si_ref(db->index, SI_REFBE);
			task->db = db;
			return 1;
		case 0: /* state 3 */
			if (sc_end(s, db, SI_BACKUP)) {
				rc = sc_backupend(s, w);
				if (ssunlikely(rc == -1)) {
					sc_backuperror(s);
					goto backup_error;
				}
				s->backup_events++;
				task->gc = 1;
				task->on_backup = 1;
			}
			break;
		}
backup_error:;
	}

	/* garbage-collection */
	if (s->gc) {
		task->plan.plan = SI_GC;
		task->plan.a = vlsn;
		task->plan.b = zone->gc_wm;
		rc = sc_planquota(s, &task->plan, SC_QGC, zone->gc_prio);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			si_ref(db->index, SI_REFBE);
			db->workers[SC_QGC]++;
			task->db = db;
			return 1;
		case 0: /* complete */
			if (sc_end(s, db, SI_GC)) {
				s->gc = 0;
				s->gc_time = now;
			}
			break;
		}
	}

	/* lru */
	if (s->lru) {
		task->plan.plan = SI_LRU;
		rc = sc_planquota(s, &task->plan, SC_QLRU, zone->lru_prio);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			si_ref(db->index, SI_REFBE);
			db->workers[SC_QLRU]++;
			task->db = db;
			return 1;
		case 0: /* complete */
			if (sc_end(s, db, SI_LRU)) {
				s->lru = 0;
				s->lru_time = now;
			}
			break;
		}
	}

	/* index aging */
	if (s->age) {
		task->plan.plan = SI_AGE;
		task->plan.a = zone->branch_age * 1000000; /* ms */
		task->plan.b = zone->branch_age_wm;
		rc = sc_planquota(s, &task->plan, SC_QBRANCH, zone->branch_prio);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			si_ref(db->index, SI_REFBE);
			db->workers[SC_QBRANCH]++;
			task->db = db;
			return 1;
		case 0: /* complete */
			if (sc_end(s, db, SI_AGE)) {
				s->age = 0;
				s->age_time = now;
			}
			break;
		}
	}

	/* compact_index (compaction with in-memory index) */
	if (zone->mode == 0) {
		task->plan.plan = SI_COMPACT_INDEX;
		task->plan.a = zone->branch_wm;
		rc = sc_plan(s, &task->plan);
		if (rc == 1) {
			si_ref(db->index, SI_REFBE);
			task->db = db;
			task->gc = 1;
			return 1;
		}
		goto no_job;
	}

	/* branching */
	task->plan.plan = SI_BRANCH;
	task->plan.a = zone->branch_wm;
	rc = sc_planquota(s, &task->plan, SC_QBRANCH, zone->branch_prio);
	if (rc == 1) {
		db->workers[SC_QBRANCH]++;
		si_ref(db->index, SI_REFBE);
		task->db = db;
		task->gc = 1;
		return 1;
	}

	/* compaction */
	task->plan.plan = SI_COMPACT;
	task->plan.a = zone->compact_wm;
	task->plan.b = zone->compact_mode;
	rc = sc_plan(s, &task->plan);
	if (rc == 1) {
		si_ref(db->index, SI_REFBE);
		task->db = db;
		return 1;
	}

no_job:
	si_planinit(&task->plan);
	return 0;
}

static inline void
sc_schedule_periodic(sc *s, sctask *task, srzone *zone, uint64_t now)
{
	/* log gc and rotation */
	if (s->rotate == 0) {
		task->rotate = 1;
		s->rotate = 1;
	}
	/* checkpoint */
	switch (zone->mode) {
	case 0:  /* compact_index */
		break;
	case 1:  /* compact_index + branch_count prio */
		assert(0);
		break;
	case 2:  /* checkpoint */
	{
		if (s->checkpoint == 0) {
			uint64_t lsn = sr_seq(s->r->seq, SR_LSN);
			s->checkpoint_lsn = lsn;
			s->checkpoint = 1;
			sc_start(s, SI_CHECKPOINT);
		}
		break;
	}
	default: /* branch + compact */
		assert(zone->mode == 3);
	}
	/* anti-cache */
	if (s->anticache == 0 && zone->anticache_period) {
		if ((now - s->anticache_time) >= zone->anticache_period_us) {
			s->anticache = 1;
			s->anticache_storage = s->anticache_limit;
			s->anticache_asn = sr_seq(s->r->seq, SR_ASNNEXT);
			sc_start(s, SI_ANTICACHE);
		}
	}
	/* snapshot */
	if (s->snapshot == 0 && zone->snapshot_period) {
		if ((now - s->snapshot_time) >= zone->snapshot_period_us) {
			s->snapshot = 1;
			s->snapshot_ssn = sr_seq(s->r->seq, SR_SSNNEXT);
			sc_start(s, SI_SNAPSHOT);
		}
	}
	/* gc */
	if (s->gc == 0 && zone->gc_prio && zone->gc_period) {
		if ((now - s->gc_time) >= zone->gc_period_us) {
			s->gc = 1;
			sc_start(s, SI_GC);
		}
	}
	/* lru */
	if (s->lru == 0 && zone->lru_prio && zone->lru_period) {
		if ((now - s->lru_time) >= zone->lru_period_us) {
			s->lru = 1;
			sc_start(s, SI_LRU);
		}
	}
	/* aging */
	if (s->age == 0 && zone->branch_prio && zone->branch_age_period) {
		if ((now - s->age_time) >= zone->branch_age_period_us) {
			s->age = 1;
			sc_start(s, SI_AGE);
		}
	}
}

static int
sc_schedule(sc *s, sctask *task, scworker *w, uint64_t vlsn)
{
	uint64_t now = ss_utime();
	srzone *zone = sr_zoneof(s->r);
	int rc;
	ss_mutexlock(&s->lock);
	sc_schedule_periodic(s, task, zone, now);
	/* database shutdown-drop */
	rc = sc_do_shutdown(s, task);
	if (rc) {
		ss_mutexunlock(&s->lock);
		return rc;
	}
	/* peek a database */
	scdb *db = sc_peek(s);
	if (ssunlikely(db == NULL)) {
		ss_mutexunlock(&s->lock);
		return 0;
	}
	rc = sc_do(s, task, w, zone, db, vlsn, now);
	/* schedule next database */
	sc_next(s);
	ss_mutexunlock(&s->lock);
	return rc;
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
	if (sdb)
		si_unref(sdb->index, SI_REFBE);
	if (t->rotate == 1)
		s->rotate = 0;
	ss_mutexunlock(&s->lock);
	return 0;
}

static inline void
sc_taskinit(sctask *task)
{
	si_planinit(&task->plan);
	task->on_backup = 0;
	task->rotate = 0;
	task->gc = 0;
	task->db = NULL;
	task->shutdown = NULL;
}

int sc_step(sc *s, scworker *w, uint64_t vlsn)
{
	sctask task;
	sc_taskinit(&task);
	int rc = sc_schedule(s, &task, w, vlsn);
	int rc_job = rc;
	if (task.rotate) {
		rc = sc_rotate(s, w);
		if (ssunlikely(rc == -1))
			goto error;
	}
	/* trigger backup competion */
	if (task.on_backup)
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
