
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libso.h>
#include <sophia.h>

int so_scheduler_branch(void *arg)
{
	sodb *db = arg;
	sdc dc;
	sd_cinit(&dc, &db->r);
	int rc;
	while (1) {
		uint64_t vlsn = sm_vlsn(&db->mvcc);
		siplan plan = {
			.plan      = SI_BRANCH,
			.condition = SI_BRANCH_SIZE,
			.a         = db->e->ctl.node_branch_wm,
			.b         = 0,
			.node      = NULL
		};
		rc = si_plan(&db->index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(&db->index, &db->r, &dc, &plan, vlsn);
		if (srunlikely(rc == -1))
			break;
	}
	sd_cfree(&dc, &db->r);
	return rc;
}

int so_scheduler_compact(void *arg)
{
	sodb *db = arg;
	sdc dc;
	sd_cinit(&dc, &db->r);
	int rc;
	while (1) {
		uint64_t vlsn = sm_vlsn(&db->mvcc);
		siplan plan = {
			.plan      = SI_COMPACT,
			.condition = SI_COMPACT_DEEP,
			.a         = db->e->ctl.node_compact_wm,
			.b         = 0,
			.node      = NULL
		};
		rc = si_plan(&db->index, &plan);
		if (rc == 0)
			break;
		rc = si_execute(&db->index, &db->r, &dc, &plan, vlsn);
		if (srunlikely(rc == -1))
			break;
	}
	sd_cfree(&dc, &db->r);
	return rc;
}

int so_scheduler_init(soscheduler *s, void *env)
{
	sr_spinlockinit(&s->lock);
	s->branch       = 0;
	s->branch_limit = 0;
	s->rotate       = 0;
	s->i            = NULL;
	s->count        = 0;
	s->rr           = 0;
	s->env          = env;
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
	sr_spinlockfree(&s->lock);
	return rcret;
}

int so_scheduler_add(soscheduler *s , void *db)
{
	sr_spinlock(&s->lock);
	so *e = s->env;
	int count = s->count + 1;
	void **i = sr_malloc(&e->a, count * sizeof(void*));
	if (srunlikely(i == NULL)) {
		sr_spinunlock(&s->lock);
		return -1;
	}
	memcpy(i, s->i, s->count * sizeof(void*));
	i[s->count] = db;
	void *iprev = s->i;
	s->i = i;
	s->count = count;
	sr_spinunlock(&s->lock);
	if (iprev)
		sr_free(&e->a, iprev);
	return 0;
}

int so_scheduler_del(soscheduler *s, void *db)
{
	if (srunlikely(s->i == NULL))
		return 0;
	sr_spinlock(&s->lock);
	so *e = s->env;
	int count = s->count - 1;
	if (srunlikely(count == 0)) {
		s->count = 0;
		sr_free(&e->a, s->i);
		s->i = NULL;
		sr_spinunlock(&s->lock);
		return 0;
	}
	void **i = sr_malloc(&e->a, count * sizeof(void*));
	if (srunlikely(i == NULL)) {
		sr_spinunlock(&s->lock);
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
	sr_spinunlock(&s->lock);
	sr_free(&e->a, iprev);
	return 0;
}

static inline void so_sleep(void)
{
	struct timespec ts;
	ts.tv_sec  = 0;
	ts.tv_nsec = 10000000; /* 10 ms */
	nanosleep(&ts, NULL);
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
			so_sleep();
	}
	return NULL;
}

int so_scheduler_run(soscheduler *s)
{
	so *e = s->env;
	s->branch_limit = 1;
	int rc;
	rc = so_workersnew(&s->workers, &e->r, e->ctl.threads,
	                   so_worker, e);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static sodb*
so_schedule_plan(soscheduler *s, siplan *plan)
{
	int start = s->rr;
	int limit = s->count;
	int i = start;
	int rc;
first_half:
	while (i < limit) {
		sodb *db = s->i[i];
		if (srunlikely(! so_dbactive(db))) {
			i++;
			continue;
		}
		rc = si_plan(&db->index, plan);
		if (rc) {
			s->rr = i;
			return db;
		}
		i++;
	}
	if (i > start) {
		i = 0;
		limit = start;
		goto first_half;
	}
	s->rr = 0;
	return NULL;
}

static int
so_schedule(soscheduler *s, sotask *task, soworker *w)
{
	sr_trace(&w->trace, "%s", "schedule");

	sr_spinlock(&s->lock);
	so *e = s->env;

	/* todo: qos-limit: (if > /2 increse max job count) */
	/* todo: snapshot, branch force by lsn */

	/* schedule log rotation and log gc task */
	task->rotate = 0;
	if (s->rotate == 0) {
		s->rotate = 1;
		task->rotate = 1;
	}

	task->plan.plan = SI_NONE;
	sodb *db;
	if (s->branch < s->branch_limit)
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
		 */
		task->plan.plan = SI_BRANCH;
		task->plan.condition = SI_BRANCH_SIZE; /* | ttl */
		task->plan.a = e->ctl.node_branch_wm;
		task->plan.b = 0;
		task->plan.node = NULL;
		db = so_schedule_plan(s, &task->plan);
		if (db) {
			s->branch++;
			task->db = db;
			sr_spinunlock(&s->lock);
			return 1;
		}
	}
	/* schedule compaction task.
	 *
	 * peek node with the largest branches count
	 */
	task->plan.plan = SI_COMPACT;
	task->plan.condition = SI_COMPACT_DEEP;
	task->plan.a = e->ctl.node_compact_wm;
	task->plan.b = 0;
	task->plan.node = NULL;
	db = so_schedule_plan(s, &task->plan);
	if (db) {
		task->db = db;
		sr_spinunlock(&s->lock);
		return 1;
	}
	sr_spinunlock(&s->lock);
	return 0;
}

static int
so_rotate(soscheduler *s, soworker *w)
{
	sr_trace(&w->trace, "%s", "log gc");
	so *e = s->env;
	int rc = sl_poolgc(&e->lp);
	if (srunlikely(rc == -1))
		return -1;
	rc = sl_poolrotate_ready(&e->lp, e->ctl.log_rotate_wm);
	if (rc) {
		sr_trace(&w->trace, "%s", "log rotation");
		rc = sl_poolrotate(&e->lp);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static int
so_execute(soscheduler *s, sotask *t, soworker *w)
{
	si_plannertrace(&t->plan, &w->trace);
	sodb *db = t->db;
	uint64_t vlsn = sm_vlsn(&db->mvcc);
	int rc = si_execute(&db->index, &db->r, &w->dc, &t->plan, vlsn);
	if (srunlikely(rc == -1))
		so_dbmalfunction(db);
	if (t->plan.plan == SI_BRANCH) {
		sr_spinlock(&s->lock);
		s->branch--;
		sr_spinunlock(&s->lock);
	}
	return rc;
}

static int
so_complete(soscheduler *s, sotask *t)
{
	sr_spinlock(&s->lock);
	if (t->plan.plan == SI_BRANCH)
		s->branch--;
	if (t->rotate == 1)
		s->rotate = 0;
	sr_spinunlock(&s->lock);
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
	if (job) {
		rc = so_execute(s, &task, w);
		if (srunlikely(rc == -1))
			goto error;
	}
	so_complete(s, &task);
	sr_trace(&w->trace, "%s", "sleep");
	return rc;
error:
	sr_trace(&w->trace, "%s", "malfunction");
	return -1;
}
