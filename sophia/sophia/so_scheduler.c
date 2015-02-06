
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libse.h>
#include <libso.h>

static inline sizone*
so_zoneof(so *e)
{
	int p = sr_quotaused_percent(&e->quota);
	return si_zonemap(&e->ctl.zones, p);
}

int so_scheduler_branch(void *arg)
{
	sodb *db = arg;
	so *e = so_of(&db->o);
	sizone *z = so_zoneof(e);
	soworker stub;
	so_workerstub_init(&stub, &db->r);
	int rc;
	while (1) {
		uint64_t vlsn = sx_vlsn(&e->xm);
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
		rc = si_execute(&db->index, &db->r, &stub.dc, &plan, vlsn);
		if (srunlikely(rc == -1))
			break;
	}
	so_workerstub_free(&stub, &db->r);
	return rc;
}

int so_scheduler_compact(void *arg)
{
	sodb *db = arg;
	so *e = so_of(&db->o);
	sizone *z = so_zoneof(e);
	soworker stub;
	so_workerstub_init(&stub, &db->r);
	int rc;
	while (1) {
		uint64_t vlsn = sx_vlsn(&e->xm);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_COMPACT,
			.a         = z->compact_wm,
			.b         = 0,
			.c         = 0,
			.node      = NULL
		};
		rc = si_plan(&db->index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(&db->index, &db->r, &stub.dc, &plan, vlsn);
		if (srunlikely(rc == -1))
			break;
	}
	so_workerstub_free(&stub, &db->r);
	return rc;
}

int so_scheduler_checkpoint(void *arg)
{
	so *o = arg;
	soscheduler *s = &o->sched;
	uint64_t lsn = sr_seq(&o->seq, SR_LSN);
	sr_mutexlock(&s->lock);
	s->checkpoint_lsn = lsn;
	s->checkpoint = 1;
	sr_mutexunlock(&s->lock);
	return 0;
}

int so_scheduler_gc(void *arg)
{
	so *o = arg;
	soscheduler *s = &o->sched;
	sr_mutexlock(&s->lock);
	s->gc = 1;
	sr_mutexunlock(&s->lock);
	return 0;
}

int so_scheduler_backup(void *arg)
{
	so *e = arg;
	soscheduler *s = &e->sched;
	if (srunlikely(e->ctl.backup_path == NULL)) {
		sr_error(&e->error, "%s", "backup is not enabled");
		return -1;
	}
	/* begin backup procedure
	 * state 0
	 *
	 * disable log garbage-collection
	*/
	sl_poolgc_enable(&e->lp, 0);
	sr_mutexlock(&s->lock);
	if (srunlikely(s->backup > 0)) {
		sr_mutexunlock(&s->lock);
		sl_poolgc_enable(&e->lp, 1);
		/* in progress */
		return 0;
	}
	uint64_t bsn = sr_seq(&e->seq, SR_BSNNEXT);
	s->backup = 1;
	s->backup_bsn = bsn;
	sr_mutexunlock(&s->lock);
	return 0;
}

static inline int
so_backupstart(soscheduler *s)
{
	so *e = s->env;
	/*
	 * a. create backup_path/<bsn.incomplete> directory
	 * b. create database directories
	 * c. create log directory
	*/
	char path[1024];
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete",
	         e->ctl.backup_path, s->backup_bsn);
	int rc = sr_filemkdir(path);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "backup directory '%s' create error: %s",
		         path, strerror(errno));
		return -1;
	}
	int i = 0;
	while (i < s->count) {
		sodb *db = s->i[i];
		snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/%s",
		         e->ctl.backup_path, s->backup_bsn,
		         db->ctl.name);
		rc = sr_filemkdir(path);
		if (srunlikely(rc == -1)) {
			sr_error(&e->error, "backup directory '%s' create error: %s",
			         path, strerror(errno));
			return -1;
		}
		i++;
	}
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/log",
	         e->ctl.backup_path, s->backup_bsn);
	rc = sr_filemkdir(path);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "backup directory '%s' create error: %s",
		         path, strerror(errno));
		return -1;
	}
	return 0;
}

static inline int
so_backupcomplete(soscheduler *s, soworker *w)
{
	/*
	 * a. rotate log file
	 * b. copy log files
	 * c. enable log gc
	 * d. rename <bsn.incomplete> into <bsn>
	 * e. set last backup, set COMPLETE
	 */
	so *e = s->env;

	/* force log rotation */
	sr_trace(&w->trace, "%s", "log rotation for backup");
	int rc = sl_poolrotate(&e->lp);
	if (srunlikely(rc == -1))
		return -1;

	/* copy log files */
	sr_trace(&w->trace, "%s", "log files backup");

	char path[1024];
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/log",
	         e->ctl.backup_path, s->backup_bsn);
	rc = sl_poolcopy(&e->lp, path, &w->dc.c);
	if (srunlikely(rc == -1)) {
		sr_errorrecover(&e->error);
		return -1;
	}

	/* enable log gc */
	sl_poolgc_enable(&e->lp, 1);

	/* complete backup */
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete",
	         e->ctl.backup_path, s->backup_bsn);
	char newpath[1024];
	snprintf(newpath, sizeof(newpath), "%s/%" PRIu32,
	         e->ctl.backup_path, s->backup_bsn);
	rc = rename(path, newpath);
	if (srunlikely(rc == -1)) {
		sr_error(&e->error, "backup directory '%s' rename error: %s",
		         path, strerror(errno));
		return -1;
	}

	/* complete */
	s->backup_last = s->backup_bsn;
	s->backup_last_complete = 1;
	s->backup = 0;
	s->backup_bsn = 0;
	return 0;
}

static inline int
so_backuperror(soscheduler *s)
{
	so *e = s->env;
	sl_poolgc_enable(&e->lp, 1);
	s->backup = 0;
	s->backup_last_complete = 0;
	return 0;
}

int so_scheduler_call(void *arg)
{
	so *e = arg;
	soscheduler *s = &e->sched;
	soworker stub;
	so_workerstub_init(&stub, &e->r);
	int rc = so_scheduler(s, &stub);
	so_workerstub_free(&stub, &e->r);
	return rc;
}

int so_scheduler_init(soscheduler *s, void *env)
{
	sr_mutexinit(&s->lock);
	s->workers_branch       = 0;
	s->workers_backup       = 0;
	s->workers_gc           = 0;
	s->workers_gc_db        = 0;
	s->rotate               = 0;
	s->i                    = NULL;
	s->count                = 0;
	s->rr                   = 0;
	s->env                  = env;
	s->checkpoint_lsn       = 0;
	s->checkpoint_lsn_last  = 0;
	s->checkpoint           = 0;
	s->age                  = 0;
	s->age_last             = 0;
	s->backup_bsn           = 0;
	s->backup_last          = 0;
	s->backup_last_complete = 0;
	s->backup               = 0;
	s->gc                   = 0;
	s->gc_last              = 0;
	so_workersinit(&s->workers);
	return 0;
}

int so_scheduler_shutdown(soscheduler *s)
{
	so *e = s->env;
	int rcret = 0;
	int rc = so_workersshutdown(&s->workers, &e->r);
	if (srunlikely(rc == -1))
		rcret = -1;
	if (s->i) {
		sr_free(&e->a, s->i);
		s->i = NULL;
	}
	sr_mutexfree(&s->lock);
	return rcret;
}

int so_scheduler_add(soscheduler *s , void *db)
{
	sr_mutexlock(&s->lock);
	so *e = s->env;
	int count = s->count + 1;
	void **i = sr_malloc(&e->a, count * sizeof(void*));
	if (srunlikely(i == NULL)) {
		sr_mutexunlock(&s->lock);
		return -1;
	}
	memcpy(i, s->i, s->count * sizeof(void*));
	i[s->count] = db;
	void *iprev = s->i;
	s->i = i;
	s->count = count;
	sr_mutexunlock(&s->lock);
	if (iprev)
		sr_free(&e->a, iprev);
	return 0;
}

int so_scheduler_del(soscheduler *s, void *db)
{
	if (srunlikely(s->i == NULL))
		return 0;
	sr_mutexlock(&s->lock);
	so *e = s->env;
	int count = s->count - 1;
	if (srunlikely(count == 0)) {
		s->count = 0;
		sr_free(&e->a, s->i);
		s->i = NULL;
		sr_mutexunlock(&s->lock);
		return 0;
	}
	void **i = sr_malloc(&e->a, count * sizeof(void*));
	if (srunlikely(i == NULL)) {
		sr_mutexunlock(&s->lock);
		return -1;
	}
	int j = 0;
	int k = 0;
	while (j < s->count) {
		if (s->i[j] == db) {
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
	if (srunlikely(s->rr >= s->count))
		s->rr = 0;
	sr_mutexunlock(&s->lock);
	sr_free(&e->a, iprev);
	return 0;
}

static void *so_worker(void *arg)
{
	soworker *self = arg;
	so *o = self->arg;
	for (;;)
	{
		int rc = so_active(o);
		if (srunlikely(rc == 0))
			break;
		rc = so_scheduler(&o->sched, self);
		if (srunlikely(rc == -1))
			break;
		if (srunlikely(rc == 0))
			sr_sleep(10000000); /* 10ms */
	}
	return NULL;
}

int so_scheduler_run(soscheduler *s)
{
	so *e = s->env;
	int rc;
	rc = so_workersnew(&s->workers, &e->r, e->ctl.threads,
	                   so_worker, e);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static int
so_schedule_plan(soscheduler *s, siplan *plan, sodb **dbret)
{
	int start = s->rr;
	int limit = s->count;
	int i = start;
	int rc_inprogress = 0;
	int rc;
	*dbret = NULL;
first_half:
	while (i < limit) {
		sodb *db = s->i[i];
		if (srunlikely(! so_dbactive(db))) {
			i++;
			continue;
		}
		rc = si_plan(&db->index, plan);
		switch (rc) {
		case 1:
			s->rr = i;
			*dbret = db;
			return 1;
		case 2: rc_inprogress = rc;
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
so_schedule(soscheduler *s, sotask *task, soworker *w)
{
	sr_trace(&w->trace, "%s", "schedule");
	si_planinit(&task->plan);

	uint64_t now = sr_utime();
	so *e = s->env;
	sodb *db;
	sizone *zone = so_zoneof(e);
	assert(zone != NULL);

	task->checkpoint_complete = 0;
	task->backup_complete = 0;
	task->rotate = 0;
	task->gc = 0;
	task->db = NULL;

	sr_mutexlock(&s->lock);

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
		rc = so_schedule_plan(s, &task->plan, &db);
		switch (rc) {
		case 1:
			s->workers_branch++;
			so_dbref(db, 1);
			task->db = db;
			task->gc = 1;
			sr_mutexunlock(&s->lock);
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
	case 1:  /* compact_index + branch_count prio */
		assert(0);
		break;
	case 2:  /* checkpoint */
	{
		if (in_progress) {
			sr_mutexunlock(&s->lock);
			return 0;
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
		sr_spinlock(&e->dblock);
		db = NULL;
		if (srunlikely(e->db_shutdown.n > 0)) {
			db = (sodb*)so_objindex_first(&e->db_shutdown);
			if (so_dbgarbage(db)) {
				so_objindex_unregister(&e->db_shutdown, &db->o);
			} else {
				db = NULL;
			}
		}
		sr_spinunlock(&e->dblock);
		if (srunlikely(db)) {
			if (db->ctl.dropped)
				task->plan.plan = SI_DROP;
			else
				task->plan.plan = SI_SHUTDOWN;
			s->workers_gc_db++;
			so_dbref(db, 1);
			task->db = db;
			sr_mutexunlock(&s->lock);
			return 1;
		}
	}

	/* backup */
	if (s->backup && (s->workers_backup < zone->backup_prio))
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
			rc = so_backupstart(s);
			if (srunlikely(rc == -1)) {
				so_backuperror(s);
				goto backup_error;
			}
			s->backup = 2;
		}
		/* state 2 */
		task->plan.plan = SI_BACKUP;
		task->plan.a = s->backup_bsn;
		rc = so_schedule_plan(s, &task->plan, &db);
		switch (rc) {
		case 1:
			s->workers_backup++;
			so_dbref(db, 1);
			task->db = db;
			sr_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			break;
		case 0: /* state 3 */
			rc = so_backupcomplete(s, w);
			if (srunlikely(rc == -1)) {
				so_backuperror(s);
				goto backup_error;
			}
			task->gc = 1;
			task->backup_complete = 1;
			break;
		}
backup_error:;
	}

	/* garbage-collection */
	if (s->gc) {
		if (s->workers_gc < zone->gc_prio) {
			task->plan.plan = SI_GC;
			task->plan.a = sx_vlsn(&e->xm);
			task->plan.b = zone->gc_wm;
			rc = so_schedule_plan(s, &task->plan, &db);
			switch (rc) {
			case 1:
				s->workers_gc++;
				so_dbref(db, 1);
				task->db = db;
				sr_mutexunlock(&s->lock);
				return 1;
			case 2: /* work in progress */
				break;
			case 0: /* state 3 */
				s->gc = 0;
				s->gc_last = now;
				break;
			}
		}
	} else {
		if (zone->gc_prio && zone->gc_period) {
			if ( (now - s->gc_last) >= (zone->gc_period * 1000000) ) {
				s->gc = 1;
			}
		}
	}

	/* index aging */
	if (s->age) {
		if (s->workers_branch < zone->branch_prio) {
			task->plan.plan = SI_AGE;
			task->plan.a = zone->branch_age * 1000000; /* ms */
			task->plan.b = zone->branch_age_wm;
			rc = so_schedule_plan(s, &task->plan, &db);
			switch (rc) {
			case 1:
				s->workers_branch++;
				so_dbref(db, 1);
				task->db = db;
				sr_mutexunlock(&s->lock);
				return 1;
			case 0:
				s->age = 0;
				s->age_last = now;
				break;
			}
		}
	} else {
		if (zone->branch_prio && zone->branch_age_period) {
			if ( (now - s->age_last) >= (zone->branch_age_period * 1000000) ) {
				s->age = 1;
			}
		}
	}

	/* branching */
	if (s->workers_branch < zone->branch_prio)
	{
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
		rc = so_schedule_plan(s, &task->plan, &db);
		if (rc == 1) {
			s->workers_branch++;
			so_dbref(db, 1);
			task->db = db;
			task->gc = 1;
			sr_mutexunlock(&s->lock);
			return 1;
		}
	}

	/* compaction */
	task->plan.plan = SI_COMPACT;
	task->plan.a = zone->compact_wm;
	rc = so_schedule_plan(s, &task->plan, &db);
	if (rc == 1) {
		so_dbref(db, 1);
		task->db = db;
		sr_mutexunlock(&s->lock);
		return 1;
	}

	sr_mutexunlock(&s->lock);
	return 0;
}

static int
so_gc(soscheduler *s, soworker *w)
{
	sr_trace(&w->trace, "%s", "log gc");
	so *e = s->env;
	int rc = sl_poolgc(&e->lp);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static int
so_rotate(soscheduler *s, soworker *w)
{
	sr_trace(&w->trace, "%s", "log rotation");
	so *e = s->env;
	int rc = sl_poolrotate_ready(&e->lp, e->ctl.log_rotate_wm);
	if (rc) {
		rc = sl_poolrotate(&e->lp);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static int
so_execute(sotask *t, soworker *w)
{
	si_plannertrace(&t->plan, &w->trace);
	sodb *db = t->db;
	so *e = (so*)db->o.env;
	uint64_t vlsn = sx_vlsn(&e->xm);
	return si_execute(&db->index, &db->r, &w->dc, &t->plan, vlsn);
}

static int
so_complete(soscheduler *s, sotask *t)
{
	sr_mutexlock(&s->lock);
	sodb *db = t->db;
	if (db)
		so_dbunref(db, 1);
	switch (t->plan.plan) {
	case SI_BRANCH:
	case SI_AGE:
	case SI_CHECKPOINT:
		s->workers_branch--;
		break;
	case SI_BACKUP:
		s->workers_backup--;
		break;
	case SI_GC:
		s->workers_gc--;
		break;
	case SI_SHUTDOWN:
	case SI_DROP:
		s->workers_gc_db--;
		so_objdestroy(&db->o);
		break;
	}
	if (t->rotate == 1)
		s->rotate = 0;
	sr_mutexunlock(&s->lock);
	return 0;
}

int so_scheduler(soscheduler *s, soworker *w)
{
	sotask task;
	int rc = so_schedule(s, &task, w);
	int job = rc;
	if (task.rotate) {
		rc = so_rotate(s, w);
		if (srunlikely(rc == -1))
			goto error;
	}
	so *e = s->env;
	if (task.checkpoint_complete) {
		sr_triggerrun(&e->ctl.checkpoint_on_complete, &e->o);
	}
	if (task.backup_complete) {
		sr_triggerrun(&e->ctl.backup_on_complete, &e->o);
	}
	if (job) {
		rc = so_execute(&task, w);
		if (srunlikely(rc == -1)) {
			if (task.plan.plan != SI_BACKUP) {
				if (task.db)
					so_dbmalfunction(task.db);
				goto error;
			}
			sr_mutexlock(&s->lock);
			so_backuperror(s);
			sr_mutexunlock(&s->lock);
		}
	}
	if (task.gc) {
		rc = so_gc(s, w);
		if (srunlikely(rc == -1))
			goto error;
	}
	so_complete(s, &task);
	sr_trace(&w->trace, "%s", "sleep");
	return job;
error:
	sr_trace(&w->trace, "%s", "malfunction");
	return -1;
}
