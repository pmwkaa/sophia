
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
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libse.h>

static inline srzone*
se_zoneof(se *e)
{
	int p = ss_quotaused_percent(&e->quota);
	return sr_zonemap(&e->conf.zones, p);
}

int se_scheduler_branch(void *arg)
{
	sedb *db = arg;
	se *e = se_of(&db->o);
	int rc = se_active(e);
	if (ssunlikely(rc == 0))
		return 0;
	srzone *z = se_zoneof(e);
	seworker *w = se_workerpool_pop(&e->sched.workers, &e->r);
	if (ssunlikely(w == NULL))
		return -1;
	while (1) {
		uint64_t vlsn = sx_vlsn(&e->xm);
		uint64_t vlsn_lru = si_lru_vlsn(&db->index);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_BRANCH,
			.a         = z->branch_wm,
			.b         = 0,
			.c         = 0,
			.node      = NULL
		};
		rc = si_plan(&db->index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(&db->index, &w->dc, &plan, vlsn, vlsn_lru);
		if (ssunlikely(rc == -1))
			break;
	}
	se_workerpool_push(&e->sched.workers, w);
	return rc;
}

int se_scheduler_compact(void *arg)
{
	sedb *db = arg;
	se *e = se_of(&db->o);
	int rc = se_active(e);
	if (ssunlikely(rc == 0))
		return 0;
	srzone *z = se_zoneof(e);
	seworker *w = se_workerpool_pop(&e->sched.workers, &e->r);
	if (ssunlikely(w == NULL))
		return -1;
	while (1) {
		uint64_t vlsn = sx_vlsn(&e->xm);
		uint64_t vlsn_lru = si_lru_vlsn(&db->index);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_COMPACT,
			.a         = z->compact_wm,
			.b         = z->compact_mode,
			.c         = 0,
			.node      = NULL
		};
		rc = si_plan(&db->index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(&db->index, &w->dc, &plan, vlsn, vlsn_lru);
		if (ssunlikely(rc == -1))
			break;
	}
	se_workerpool_push(&e->sched.workers, w);
	return rc;
}

int se_scheduler_compact_index(void *arg)
{
	sedb *db = arg;
	se *e = se_of(&db->o);
	int rc = se_active(e);
	if (ssunlikely(rc == 0))
		return 0;
	srzone *z = se_zoneof(e);
	seworker *w = se_workerpool_pop(&e->sched.workers, &e->r);
	if (ssunlikely(w == NULL))
		return -1;
	while (1) {
		uint64_t vlsn = sx_vlsn(&e->xm);
		uint64_t vlsn_lru = si_lru_vlsn(&db->index);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_COMPACT_INDEX,
			.a         = z->branch_wm,
			.b         = 0,
			.c         = 0,
			.node      = NULL
		};
		rc = si_plan(&db->index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(&db->index, &w->dc, &plan, vlsn, vlsn_lru);
		if (ssunlikely(rc == -1))
			break;
	}
	se_workerpool_push(&e->sched.workers, w);
	return rc;
}

int se_scheduler_anticache(void *arg)
{
	se *o = arg;
	sescheduler *s = &o->sched;
	uint64_t asn = sr_seq(&o->seq, SR_ASNNEXT);
	ss_mutexlock(&s->lock);
	s->anticache_asn = asn;
	s->anticache_storage = o->conf.anticache;
	s->anticache = 1;
	ss_mutexunlock(&s->lock);
	return 0;
}

int se_scheduler_snapshot(void *arg)
{
	se *o = arg;
	sescheduler *s = &o->sched;
	uint64_t ssn = sr_seq(&o->seq, SR_SSNNEXT);
	ss_mutexlock(&s->lock);
	s->snapshot_ssn = ssn;
	s->snapshot = 1;
	ss_mutexunlock(&s->lock);
	return 0;
}

int se_scheduler_checkpoint(void *arg)
{
	se *o = arg;
	sescheduler *s = &o->sched;
	uint64_t lsn = sr_seq(&o->seq, SR_LSN);
	ss_mutexlock(&s->lock);
	s->checkpoint_lsn = lsn;
	s->checkpoint = 1;
	ss_mutexunlock(&s->lock);
	return 0;
}

int se_scheduler_gc(void *arg)
{
	se *o = arg;
	sescheduler *s = &o->sched;
	ss_mutexlock(&s->lock);
	s->gc = 1;
	ss_mutexunlock(&s->lock);
	return 0;
}

int se_scheduler_lru(void *arg)
{
	se *o = arg;
	sescheduler *s = &o->sched;
	ss_mutexlock(&s->lock);
	s->lru = 1;
	ss_mutexunlock(&s->lock);
	return 0;
}

int se_scheduler_backup(void *arg)
{
	se *e = arg;
	sescheduler *s = &e->sched;
	if (ssunlikely(e->conf.backup_path == NULL)) {
		sr_error(&e->error, "%s", "backup is not enabled");
		return -1;
	}
	/* begin backup procedure
	 * state 0
	 *
	 * disable log garbage-collection
	*/
	sl_poolgc_enable(&e->lp, 0);
	ss_mutexlock(&s->lock);
	if (ssunlikely(s->backup > 0)) {
		ss_mutexunlock(&s->lock);
		sl_poolgc_enable(&e->lp, 1);
		/* in progress */
		return 0;
	}
	uint64_t bsn = sr_seq(&e->seq, SR_BSNNEXT);
	s->backup = 1;
	s->backup_bsn = bsn;
	ss_mutexunlock(&s->lock);
	return 0;
}

static inline int
se_backupstart(sescheduler *s)
{
	se *e = (se*)s->env;
	/*
	 * a. create backup_path/<bsn.incomplete> directory
	 * b. create database directories
	 * c. create log directory
	*/
	char path[1024];
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete",
	         e->conf.backup_path, s->backup_bsn);
	int rc = ss_vfsmkdir(&e->vfs, path, 0755);
	if (ssunlikely(rc == -1)) {
		sr_error(&e->error, "backup directory '%s' create error: %s",
		         path, strerror(errno));
		return -1;
	}
	int i = 0;
	while (i < s->count) {
		seschedulerdb *sdb = &s->i[i];
		sedb *db = sdb->db;
		snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/%s",
		         e->conf.backup_path, s->backup_bsn,
		         db->scheme.name);
		rc = ss_vfsmkdir(&e->vfs, path, 0755);
		if (ssunlikely(rc == -1)) {
			sr_error(&e->error, "backup directory '%s' create error: %s",
			         path, strerror(errno));
			return -1;
		}
		i++;
	}
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/log",
	         e->conf.backup_path, s->backup_bsn);
	rc = ss_vfsmkdir(&e->vfs, path, 0755);
	if (ssunlikely(rc == -1)) {
		sr_error(&e->error, "backup directory '%s' create error: %s",
		         path, strerror(errno));
		return -1;
	}
	return 0;
}

static inline int
se_backupcomplete(sescheduler *s, seworker *w)
{
	/*
	 * a. rotate log file
	 * b. copy log files
	 * c. enable log gc
	 * d. rename <bsn.incomplete> into <bsn>
	 * e. set last backup, set COMPLETE
	 */
	se *e = (se*)s->env;

	/* force log rotation */
	ss_trace(&w->trace, "%s", "log rotation for backup");
	int rc = sl_poolrotate(&e->lp);
	if (ssunlikely(rc == -1))
		return -1;

	/* copy log files */
	ss_trace(&w->trace, "%s", "log files backup");

	char path[1024];
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/log",
	         e->conf.backup_path, s->backup_bsn);
	rc = sl_poolcopy(&e->lp, path, &w->dc.c);
	if (ssunlikely(rc == -1)) {
		sr_errorrecover(&e->error);
		return -1;
	}

	/* enable log gc */
	sl_poolgc_enable(&e->lp, 1);

	/* complete backup */
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete",
	         e->conf.backup_path, s->backup_bsn);
	char newpath[1024];
	snprintf(newpath, sizeof(newpath), "%s/%" PRIu32,
	         e->conf.backup_path, s->backup_bsn);
	rc = rename(path, newpath);
	if (ssunlikely(rc == -1)) {
		sr_error(&e->error, "backup directory '%s' rename error: %s",
		         path, strerror(errno));
		return -1;
	}

	/* complete */
	s->backup_bsn_last = s->backup_bsn;
	s->backup_bsn_last_complete = 1;
	s->backup = 0;
	s->backup_bsn = 0;
	return 0;
}

static inline int
se_backuperror(sescheduler *s)
{
	se *e = (se*)s->env;
	sl_poolgc_enable(&e->lp, 1);
	s->backup = 0;
	s->backup_bsn_last_complete = 0;
	return 0;
}

int se_scheduler_call(void *arg)
{
	se *e = arg;
	sescheduler *s = &e->sched;
	int rc = se_active(e);
	if (ssunlikely(rc == 0))
		return 0;
	seworker *w = se_workerpool_pop(&e->sched.workers, &e->r);
	if (ssunlikely(w == NULL))
		return -1;
	rc = se_scheduler(s, w);
	se_workerpool_push(&e->sched.workers, w);
	return rc;
}

int se_scheduler_init(sescheduler *s, so *env)
{
	uint64_t now = ss_utime();
	ss_mutexinit(&s->lock);
	s->workers_gc_db            = 0;
	s->rotate                   = 0;
	s->req                      = 0;
	s->i                        = NULL;
	s->count                    = 0;
	s->rr                       = 0;
	s->env                      = env;
	s->checkpoint_lsn           = 0;
	s->checkpoint_lsn_last      = 0;
	s->checkpoint               = 0;
	s->age                      = 0;
	s->age_time                 = now;
	s->backup_bsn               = 0;
	s->backup_bsn_last          = 0;
	s->backup_bsn_last_complete = 0;
	s->backup_events            = 0;
	s->backup                   = 0;
	s->anticache_asn            = 0;
	s->anticache_asn_last       = 0;
	s->anticache_storage        = 0;
	s->anticache_time           = now;
	s->anticache                = 0;
	s->snapshot_ssn             = 0;
	s->snapshot_ssn_last        = 0;
	s->snapshot_time            = now;
	s->snapshot                 = 0;
	s->gc                       = 0;
	s->gc_time                  = now;
	s->lru                      = 0;
	s->lru_time                 = now;
	ss_threadpool_init(&s->tp);
	se_workerpool_init(&s->workers);
	return 0;
}

int se_scheduler_shutdown(sescheduler *s)
{
	se *e = (se*)s->env;
	se_reqwakeup(e);
	int rcret = 0;
	int rc = ss_threadpool_shutdown(&s->tp, &e->a);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = se_workerpool_free(&s->workers, &e->r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	if (s->i) {
		ss_free(&e->a, s->i);
		s->i = NULL;
	}
	ss_mutexfree(&s->lock);
	return rcret;
}

static inline void
se_scheduler_prepare(seschedulerdb *db, void *dbptr)
{
	db->db = dbptr;
	memset(db->workers, 0, sizeof(db->workers));
}

int se_scheduler_add(sescheduler *s , void *db)
{
	ss_mutexlock(&s->lock);
	se *e = (se*)s->env;
	int count = s->count + 1;
	seschedulerdb *i = ss_malloc(&e->a, count * sizeof(seschedulerdb));
	if (ssunlikely(i == NULL)) {
		ss_mutexunlock(&s->lock);
		return -1;
	}
	memcpy(i, s->i, s->count * sizeof(seschedulerdb));
	se_scheduler_prepare(&i[s->count], db);
	void *iprev = s->i;
	s->i = i;
	s->count = count;
	ss_mutexunlock(&s->lock);
	if (iprev)
		ss_free(&e->a, iprev);
	return 0;
}

int se_scheduler_del(sescheduler *s, void *db)
{
	if (ssunlikely(s->i == NULL))
		return 0;
	ss_mutexlock(&s->lock);
	se *e = (se*)s->env;
	int count = s->count - 1;
	if (ssunlikely(count == 0)) {
		s->count = 0;
		ss_free(&e->a, s->i);
		s->i = NULL;
		ss_mutexunlock(&s->lock);
		return 0;
	}
	seschedulerdb *i = ss_malloc(&e->a, count * sizeof(seschedulerdb));
	if (ssunlikely(i == NULL)) {
		ss_mutexunlock(&s->lock);
		return -1;
	}
	int j = 0;
	int k = 0;
	while (j < s->count) {
		if (s->i[j].db == db) {
			j++;
			continue;
		}
		i[k] = s->i[j];
		k++;
		j++;
	}
	void *iprev = s->i;
	s->i = i;
	s->count = count;
	if (ssunlikely(s->rr >= s->count))
		s->rr = 0;
	ss_mutexunlock(&s->lock);
	ss_free(&e->a, iprev);
	return 0;
}

static void *se_worker(void *arg)
{
	ssthread *self = arg;
	se *e = self->arg;
	seworker *worker = se_workerpool_pop(&e->sched.workers, &e->r);
	if (ssunlikely(worker == NULL))
		return NULL;
	for (;;)
	{
		int rc = se_active(e);
		if (ssunlikely(rc == 0))
			break;
		rc = se_scheduler(&e->sched, worker);
		if (ssunlikely(rc == -1))
			break;
		if (ssunlikely(rc == 0))
			ss_sleep(10000000); /* 10ms */
	}
	se_workerpool_push(&e->sched.workers, worker);
	return NULL;
}

int se_scheduler_run(sescheduler *s)
{
	se *e = (se*)s->env;
	int rc;
	rc = ss_threadpool_new(&s->tp, &e->a, e->conf.threads,
	                       se_worker, e);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static int
se_schedule_plan(sescheduler *s, siplan *plan,
                 uint32_t quota, uint32_t quota_limit,
                 seschedulerdb **dbret)
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
		seschedulerdb *db = &s->i[i];
		if (ssunlikely(! se_dbactive(db->db))) {
			i++;
			continue;
		}
		if (quota != SE_SCHEDQ_NONE) {
			if (db->workers[quota] >= quota_limit) {
				rc_inprogress = 2;
				i++;
				continue;
			}
		}
		rc = si_plan(&((sedb*)db->db)->index, plan);
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
se_schedule(sescheduler *s, setask *task, seworker *w)
{
	ss_trace(&w->trace, "%s", "schedule");
	uint64_t now = ss_utime();

	se *e = (se*)s->env;
	srzone *zone = se_zoneof(e);
	seschedulerdb *sdb = NULL;

	si_planinit(&task->plan);
	task->checkpoint_complete = 0;
	task->anticache_complete = 0;
	task->snapshot_complete = 0;
	task->backup_complete = 0;
	task->rotate = 0;
	task->req = 0;
	task->gc = 0;
	task->db = NULL;
	task->db_gc = NULL;

	ss_mutexlock(&s->lock);

	/* asynchronous reqs dispatcher */
	if (s->req == 0) {
		switch (zone->async) {
		case 2:
			if (se_reqqueue(e) == 0)
				break;
		case 1:
			s->req = 1;
			task->req = zone->async;
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
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_NONE, 0, &sdb);
		switch (rc) {
		case 1:
			sdb->workers[SE_SCHEDQ_BRANCH]++;
			se_dbref(sdb->db, 1);
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
		uint64_t lsn = sr_seq(&e->seq, SR_LSN);
		s->checkpoint_lsn = lsn;
		s->checkpoint = 1;
		goto checkpoint;
	}
	default: /* branch + compact */
		assert(zone->mode == 3);
	}

	/* database shutdown-drop */
	if (s->workers_gc_db < zone->gc_db_prio) {
		ss_spinlock(&e->dblock);
		sedb *db_gc = NULL;
		if (ssunlikely(e->db_shutdown.n > 0)) {
			db_gc = (sedb*)so_listfirst(&e->db_shutdown);
			if (se_dbgarbage(db_gc)) {
				so_listdel(&e->db_shutdown, &db_gc->o);
			} else {
				db_gc = NULL;
			}
		}
		ss_spinunlock(&e->dblock);
		if (ssunlikely(db_gc)) {
			if (db_gc->dropped)
				task->plan.plan = SI_DROP;
			else
				task->plan.plan = SI_SHUTDOWN;
			s->workers_gc_db++;
			se_dbref(db_gc, 1);
			task->db = NULL;
			task->db_gc = db_gc;
			ss_mutexunlock(&s->lock);
			return 1;
		}
	}

	/* anti-cache */
	if (s->anticache) {
		task->plan.plan = SI_ANTICACHE;
		task->plan.a = s->anticache_asn;
		task->plan.b = s->anticache_storage;
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_NONE, 0, &sdb);
		switch (rc) {
		case 1:
			se_dbref(sdb->db, 1);
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
				s->anticache_storage = e->conf.anticache;
				s->anticache_asn = sr_seq(&e->seq, SR_ASNNEXT);
			}
		}
	}

	/* snapshot */
	if (s->snapshot) {
		task->plan.plan = SI_SNAPSHOT;
		task->plan.a = s->snapshot_ssn;
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_NONE, 0, &sdb);
		switch (rc) {
		case 1:
			se_dbref(sdb->db, 1);
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
				s->snapshot_ssn = sr_seq(&e->seq, SR_SSNNEXT);
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
			rc = se_backupstart(s);
			if (ssunlikely(rc == -1)) {
				se_backuperror(s);
				goto backup_error;
			}
			s->backup = 2;
		}
		/* state 2 */
		task->plan.plan = SI_BACKUP;
		task->plan.a = s->backup_bsn;
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_BACKUP,
		                      zone->backup_prio, &sdb);
		switch (rc) {
		case 1:
			sdb->workers[SE_SCHEDQ_BACKUP]++;
			se_dbref(sdb->db, 1);
			task->db = sdb;
			ss_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			break;
		case 0: /* state 3 */
			rc = se_backupcomplete(s, w);
			if (ssunlikely(rc == -1)) {
				se_backuperror(s);
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
		task->plan.a = sx_vlsn(&e->xm);
		task->plan.b = zone->gc_wm;
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_GC, zone->gc_prio, &sdb);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			sdb->workers[SE_SCHEDQ_GC]++;
			se_dbref(sdb->db, 1);
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
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_LRU, zone->lru_prio, &sdb);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			sdb->workers[SE_SCHEDQ_LRU]++;
			se_dbref(sdb->db, 1);
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
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_BRANCH,
		                      zone->branch_prio, &sdb);
		switch (rc) {
		case 1:
			if (zone->mode == 0)
				task->plan.plan = SI_COMPACT_INDEX;
			sdb->workers[SE_SCHEDQ_BRANCH]++;
			se_dbref(sdb->db, 1);
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
		rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_NONE, 0, &sdb);
		if (rc == 1) {
			se_dbref(sdb->db, 1);
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
	rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_BRANCH,
	                      zone->branch_prio, &sdb);
	if (rc == 1) {
		sdb->workers[SE_SCHEDQ_BRANCH]++;
		se_dbref(sdb->db, 1);
		task->db = sdb;
		task->gc = 1;
		ss_mutexunlock(&s->lock);
		return 1;
	}

	/* compaction */
	task->plan.plan = SI_COMPACT;
	task->plan.a = zone->compact_wm;
	task->plan.b = zone->compact_mode;
	rc = se_schedule_plan(s, &task->plan, SE_SCHEDQ_NONE, 0, &sdb);
	if (rc == 1) {
		se_dbref(sdb->db, 1);
		task->db = sdb;
		ss_mutexunlock(&s->lock);
		return 1;
	}

	ss_mutexunlock(&s->lock);
no_job:
	si_planinit(&task->plan);
	return 0;
}

static int
se_gc(sescheduler *s, seworker *w)
{
	ss_trace(&w->trace, "%s", "log gc");
	se *e = (se*)s->env;
	int rc = sl_poolgc(&e->lp);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static int
se_rotate(sescheduler *s, seworker *w)
{
	ss_trace(&w->trace, "%s", "log rotation");
	se *e = (se*)s->env;
	int rc = sl_poolrotate_ready(&e->lp, e->conf.log_rotate_wm);
	if (rc) {
		rc = sl_poolrotate(&e->lp);
		if (ssunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static int
se_run(setask *t, seworker *w)
{
	sedb *db = sslikely(t->db) ? t->db->db : t->db_gc;
	si_plannertrace(&t->plan, db->scheme.id, &w->trace);
	se *e = (se*)db->o.env;
	uint64_t vlsn = sx_vlsn(&e->xm);
	uint64_t vlsn_lru = si_lru_vlsn(&db->index);
	return si_execute(&db->index, &w->dc, &t->plan, vlsn, vlsn_lru);
}

static int
se_dispatch(sescheduler *s, seworker *w, setask *t)
{
	ss_trace(&w->trace, "%s", "dispatcher");
	se *e = (se*)s->env;
	int block = t->req == 1;
	do {
		int rc = se_active(e);
		if (ssunlikely(rc == 0))
			break;
		sereq *req = se_reqdispatch(e, block);
		if (req) {
			switch (req->op) {
			case SE_REQREAD:
				se_execute_read(req);
				break;
			default: assert(0);
			}
			se_reqready(req);
		}
	} while (block);
	return 0;
}

static int
se_complete(sescheduler *s, setask *t)
{
	ss_mutexlock(&s->lock);
	seschedulerdb *sdb = t->db;
	switch (t->plan.plan) {
	case SI_BRANCH:
	case SI_AGE:
	case SI_CHECKPOINT:
		sdb->workers[SE_SCHEDQ_BRANCH]--;
		break;
	case SI_COMPACT_INDEX:
		break;
	case SI_BACKUP:
	case SI_BACKUPEND:
		sdb->workers[SE_SCHEDQ_BACKUP]--;
		break;
	case SI_SNAPSHOT:
		break;
	case SI_ANTICACHE:
		break;
	case SI_GC:
		sdb->workers[SE_SCHEDQ_GC]--;
		break;
	case SI_LRU:
		sdb->workers[SE_SCHEDQ_LRU]--;
		break;
	case SI_SHUTDOWN:
	case SI_DROP:
		assert(t->db == NULL);
		s->workers_gc_db--;
		se_dbunref(t->db_gc, 1);
		so_destroy((so*)t->db_gc);
		break;
	}
	if (sdb && sdb->db)
		se_dbunref(sdb->db, 1);
	if (t->rotate == 1)
		s->rotate = 0;
	if (t->req)
		s->req = 0;
	ss_mutexunlock(&s->lock);
	return 0;
}

int se_scheduler(sescheduler *s, seworker *w)
{
	setask task;
	int rc = se_schedule(s, &task, w);
	int job = rc;
	if (task.rotate) {
		rc = se_rotate(s, w);
		if (ssunlikely(rc == -1))
			goto error;
	}
	if (task.req) {
		rc = se_dispatch(s, w, &task);
		if (ssunlikely(rc == -1)) {
			goto error;
		}
	}
	se *e = (se*)s->env;
	if (task.backup_complete)
		se_reqonbackup(e);
	if (job) {
		rc = se_run(&task, w);
		if (ssunlikely(rc == -1)) {
			if (task.plan.plan != SI_BACKUP &&
			    task.plan.plan != SI_BACKUPEND) {
				se_dbmalfunction(task.db->db);
				goto error;
			}
			ss_mutexlock(&s->lock);
			se_backuperror(s);
			ss_mutexunlock(&s->lock);
		}
	}
	if (task.gc) {
		rc = se_gc(s, w);
		if (ssunlikely(rc == -1))
			goto error;
	}
	se_complete(s, &task);
	ss_trace(&w->trace, "%s", "sleep");
	return job;
error:
	ss_trace(&w->trace, "%s", "malfunction");
	return -1;
}
