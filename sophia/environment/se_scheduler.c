
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
	return sr_zonemap(&e->meta.zones, p);
}

int se_scheduler_branch(void *arg)
{
	sedb *db = arg;
	se *e = se_of(&db->o);
	srzone *z = se_zoneof(e);
	seworker stub;
	se_workerstub_init(&stub);
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
		rc = si_execute(&db->index, &stub.dc, &plan, vlsn);
		if (ssunlikely(rc == -1))
			break;
	}
	se_workerstub_free(&stub, &db->r);
	return rc;
}

int se_scheduler_compact(void *arg)
{
	sedb *db = arg;
	se *e = se_of(&db->o);
	srzone *z = se_zoneof(e);
	seworker stub;
	se_workerstub_init(&stub);
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
		rc = si_execute(&db->index, &stub.dc, &plan, vlsn);
		if (ssunlikely(rc == -1))
			break;
	}
	se_workerstub_free(&stub, &db->r);
	return rc;
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

int se_scheduler_backup(void *arg)
{
	se *e = arg;
	sescheduler *s = &e->sched;
	if (ssunlikely(e->meta.backup_path == NULL)) {
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
	         e->meta.backup_path, s->backup_bsn);
	int rc = ss_vfsmkdir(&e->vfs, path, 0755);
	if (ssunlikely(rc == -1)) {
		sr_error(&e->error, "backup directory '%s' create error: %s",
		         path, strerror(errno));
		return -1;
	}
	int i = 0;
	while (i < s->count) {
		sedb *db = s->i[i];
		snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/%s",
		         e->meta.backup_path, s->backup_bsn,
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
	         e->meta.backup_path, s->backup_bsn);
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
	         e->meta.backup_path, s->backup_bsn);
	rc = sl_poolcopy(&e->lp, path, &w->dc.c);
	if (ssunlikely(rc == -1)) {
		sr_errorrecover(&e->error);
		return -1;
	}

	/* enable log gc */
	sl_poolgc_enable(&e->lp, 1);

	/* complete backup */
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete",
	         e->meta.backup_path, s->backup_bsn);
	char newpath[1024];
	snprintf(newpath, sizeof(newpath), "%s/%" PRIu32,
	         e->meta.backup_path, s->backup_bsn);
	rc = rename(path, newpath);
	if (ssunlikely(rc == -1)) {
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
se_backuperror(sescheduler *s)
{
	se *e = (se*)s->env;
	sl_poolgc_enable(&e->lp, 1);
	s->backup = 0;
	s->backup_last_complete = 0;
	return 0;
}

int se_scheduler_call(void *arg)
{
	se *e = arg;
	sescheduler *s = &e->sched;
	seworker stub;
	se_workerstub_init(&stub);
	int rc = se_scheduler(s, &stub);
	se_workerstub_free(&stub, &e->r);
	return rc;
}

int se_scheduler_init(sescheduler *s, so *env)
{
	ss_mutexinit(&s->lock);
	s->workers_branch       = 0;
	s->workers_backup       = 0;
	s->workers_gc           = 0;
	s->workers_gc_db        = 0;
	s->rotate               = 0;
	s->req                  = 0;
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
	s->backup_events        = 0;
	s->backup               = 0;
	s->gc                   = 0;
	s->gc_last              = 0;
	se_workerpool_init(&s->workers);
	return 0;
}

int se_scheduler_shutdown(sescheduler *s)
{
	se *e = (se*)s->env;
	se_reqwakeup(e);
	int rcret = 0;
	int rc = se_workerpool_shutdown(&s->workers, &e->r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	if (s->i) {
		ss_free(&e->a, s->i);
		s->i = NULL;
	}
	ss_mutexfree(&s->lock);
	return rcret;
}

int se_scheduler_add(sescheduler *s , void *db)
{
	ss_mutexlock(&s->lock);
	se *e = (se*)s->env;
	int count = s->count + 1;
	void **i = ss_malloc(&e->a, count * sizeof(void*));
	if (ssunlikely(i == NULL)) {
		ss_mutexunlock(&s->lock);
		return -1;
	}
	memcpy(i, s->i, s->count * sizeof(void*));
	i[s->count] = db;
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
	void **i = ss_malloc(&e->a, count * sizeof(void*));
	if (ssunlikely(i == NULL)) {
		ss_mutexunlock(&s->lock);
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
	if (ssunlikely(s->rr >= s->count))
		s->rr = 0;
	ss_mutexunlock(&s->lock);
	ss_free(&e->a, iprev);
	return 0;
}

static void *se_worker(void *arg)
{
	seworker *self = arg;
	se *o = self->arg;
	for (;;)
	{
		int rc = se_active(o);
		if (ssunlikely(rc == 0))
			break;
		rc = se_scheduler(&o->sched, self);
		if (ssunlikely(rc == -1))
			break;
		if (ssunlikely(rc == 0))
			ss_sleep(10000000); /* 10ms */
	}
	return NULL;
}

int se_scheduler_run(sescheduler *s)
{
	se *e = (se*)s->env;
	int rc;
	rc = se_workerpool_new(&s->workers, &e->r, e->meta.threads,
	                       se_worker, e);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static int
se_schedule_plan(sescheduler *s, siplan *plan, sedb **dbret)
{
	int start = s->rr;
	int limit = s->count;
	int i = start;
	int rc_inprogress = 0;
	int rc;
	*dbret = NULL;
first_half:
	while (i < limit) {
		sedb *db = s->i[i];
		if (ssunlikely(! se_dbactive(db))) {
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
se_schedule(sescheduler *s, setask *task, seworker *w)
{
	ss_trace(&w->trace, "%s", "schedule");
	si_planinit(&task->plan);

	uint64_t now = ss_utime();
	se *e = (se*)s->env;
	sedb *db;
	srzone *zone = se_zoneof(e);
	assert(zone != NULL);

	task->checkpoint_complete = 0;
	task->backup_complete = 0;
	task->rotate = 0;
	task->req = 0;
	task->gc = 0;
	task->db = NULL;

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
		rc = se_schedule_plan(s, &task->plan, &db);
		switch (rc) {
		case 1:
			s->workers_branch++;
			se_dbref(db, 1);
			task->db = db;
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
	case 1:  /* compact_index + branch_count prio */
		assert(0);
		break;
	case 2:  /* checkpoint */
	{
		if (in_progress) {
			ss_mutexunlock(&s->lock);
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
		ss_spinlock(&e->dblock);
		db = NULL;
		if (ssunlikely(e->db_shutdown.n > 0)) {
			db = (sedb*)so_listfirst(&e->db_shutdown);
			if (se_dbgarbage(db)) {
				so_listdel(&e->db_shutdown, &db->o);
			} else {
				db = NULL;
			}
		}
		ss_spinunlock(&e->dblock);
		if (ssunlikely(db)) {
			if (db->dropped)
				task->plan.plan = SI_DROP;
			else
				task->plan.plan = SI_SHUTDOWN;
			s->workers_gc_db++;
			se_dbref(db, 1);
			task->db = db;
			ss_mutexunlock(&s->lock);
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
		rc = se_schedule_plan(s, &task->plan, &db);
		switch (rc) {
		case 1:
			s->workers_backup++;
			se_dbref(db, 1);
			task->db = db;
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
		if (s->workers_gc < zone->gc_prio) {
			task->plan.plan = SI_GC;
			task->plan.a = sx_vlsn(&e->xm);
			task->plan.b = zone->gc_wm;
			rc = se_schedule_plan(s, &task->plan, &db);
			switch (rc) {
			case 1:
				s->workers_gc++;
				se_dbref(db, 1);
				task->db = db;
				ss_mutexunlock(&s->lock);
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
			if ( (now - s->gc_last) >= ((uint64_t)zone->gc_period * 1000000) ) {
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
			rc = se_schedule_plan(s, &task->plan, &db);
			switch (rc) {
			case 1:
				s->workers_branch++;
				se_dbref(db, 1);
				task->db = db;
				ss_mutexunlock(&s->lock);
				return 1;
			case 0:
				s->age = 0;
				s->age_last = now;
				break;
			}
		}
	} else {
		if (zone->branch_prio && zone->branch_age_period) {
			if ( (now - s->age_last) >= ((uint64_t)zone->branch_age_period * 1000000) ) {
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
		rc = se_schedule_plan(s, &task->plan, &db);
		if (rc == 1) {
			s->workers_branch++;
			se_dbref(db, 1);
			task->db = db;
			task->gc = 1;
			ss_mutexunlock(&s->lock);
			return 1;
		}
	}

	/* compaction */
	task->plan.plan = SI_COMPACT;
	task->plan.a = zone->compact_wm;
	rc = se_schedule_plan(s, &task->plan, &db);
	if (rc == 1) {
		se_dbref(db, 1);
		task->db = db;
		ss_mutexunlock(&s->lock);
		return 1;
	}

	ss_mutexunlock(&s->lock);
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
	int rc = sl_poolrotate_ready(&e->lp, e->meta.log_rotate_wm);
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
	si_plannertrace(&t->plan, &w->trace);
	sedb *db = t->db;
	se *e = (se*)db->o.env;
	uint64_t vlsn = sx_vlsn(&e->xm);
	return si_execute(&db->index, &w->dc, &t->plan, vlsn);
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
			se_execute(req);
			se_reqready(req);
		}
	} while (block);
	return 0;
}

static int
se_complete(sescheduler *s, setask *t)
{
	ss_mutexlock(&s->lock);
	sedb *db = t->db;
	if (db)
		se_dbunref(db, 1);
	switch (t->plan.plan) {
	case SI_BRANCH:
	case SI_AGE:
	case SI_CHECKPOINT:
		s->workers_branch--;
		break;
	case SI_BACKUP:
	case SI_BACKUPEND:
		s->workers_backup--;
		break;
	case SI_GC:
		s->workers_gc--;
		break;
	case SI_SHUTDOWN:
	case SI_DROP:
		s->workers_gc_db--;
		so_destroy(&db->o);
		break;
	}
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
				se_dbmalfunction(task.db);
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
