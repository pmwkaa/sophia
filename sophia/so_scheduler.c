
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
	sizone *z = so_zoneof(db->e);
	soworker stub;
	so_workerstub_init(&stub, &db->r);
	int rc;
	while (1) {
		uint64_t vlsn = sm_vlsn(&db->mvcc);
		siplan plan = {
			.explain   = SI_ENONE,
			.plan      = SI_BRANCH,
			.a         = z->branch_wm,
			.b         = z->branch_ttl * 1000000, /* ms */
			.c         = z->branch_ttl_wm,
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
	sizone *z = so_zoneof(db->e);
	soworker stub;
	so_workerstub_init(&stub, &db->r);
	int rc;
	while (1) {
		uint64_t vlsn = sm_vlsn(&db->mvcc);
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

int so_scheduler_call(void *arg)
{
	so *o = arg;
	soscheduler *s = &o->sched;
	soworker stub;
	so_workerstub_init(&stub, &o->r);
	int rc = so_scheduler(s, &stub);
	so_workerstub_free(&stub, &o->r);
	return rc;
}

int so_scheduler_init(soscheduler *s, void *env)
{
	sr_mutexinit(&s->lock);
	s->branch              = 0;
	s->rotate              = 0;
	s->i                   = NULL;
	s->count               = 0;
	s->rr                  = 0;
	s->env                 = env;
	s->checkpoint_lsn      = 0;
	s->checkpoint_lsn_last = 0;
	s->checkpoint          = 0;
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

	so *e = s->env;
	sodb *db;
	sizone *zone = so_zoneof(e);
	assert(zone != NULL);

	sr_mutexlock(&s->lock);

	/* log gc-rotation */
	task->rotate = 0;
	if (s->rotate == 0) {
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
			s->branch++;
			task->db = db;
			sr_mutexunlock(&s->lock);
			return 1;
		case 2: /* work in progress */
			in_progress = 1;
			break;
		case 0: /* complete checkpoint */
			s->checkpoint = 0;
			s->checkpoint_lsn_last = s->checkpoint_lsn;
			s->checkpoint_lsn = 0;
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

	if (s->branch < zone->branch_prio)
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
		task->plan.b = zone->branch_ttl * 1000000; /* ms */
		task->plan.c = zone->branch_ttl_wm;
		rc = so_schedule_plan(s, &task->plan, &db);
		if (rc == 1) {
			s->branch++;
			task->db = db;
			sr_mutexunlock(&s->lock);
			return 1;
		}
	}

	/* schedule compaction task.
	 *
	 * peek node with the largest branches count
	 */
	task->plan.plan = SI_COMPACT;
	task->plan.a = zone->compact_wm;
	rc = so_schedule_plan(s, &task->plan, &db);
	if (rc == 1) {
		task->db = db;
		sr_mutexunlock(&s->lock);
		return 1;
	}

	sr_mutexunlock(&s->lock);
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
so_execute(sotask *t, soworker *w)
{
	si_plannertrace(&t->plan, &w->trace);
	sodb *db = t->db;
	uint64_t vlsn = sm_vlsn(&db->mvcc);
	int rc = si_execute(&db->index, &db->r, &w->dc, &t->plan, vlsn);
	if (srunlikely(rc == -1))
		so_dbmalfunction(db);
	return rc;
}

static int
so_complete(soscheduler *s, sotask *t)
{
	sr_mutexlock(&s->lock);
	if (t->plan.plan == SI_BRANCH)
		s->branch--;
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
	if (job) {
		rc = so_execute(&task, w);
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
