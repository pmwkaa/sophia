
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

typedef struct {
	int checkpoint_active;
	uint64_t checkpoint_lsn;
	uint64_t checkpoint_lsn_last;
} soschedulerinfo;

static int
so_schedulerctl_checkpoint(srctl *c srunused, void *arg, va_list args srunused)
{
	return so_scheduler_checkpoint(arg);
}

static int
so_schedulerctl_run(srctl *c srunused, void *arg, va_list args srunused)
{
	return so_scheduler_call(arg);
}

static inline void
so_schedulerctl_prepare(srctl *t, so *o, soctl *c, soschedulerinfo *si)
{
	sr_spinlock(&o->sched.lock);
	si->checkpoint_active   = o->sched.checkpoint;
	si->checkpoint_lsn_last = o->sched.checkpoint_lsn_last;
	si->checkpoint_lsn      = o->sched.checkpoint_lsn;
	sr_spinunlock(&o->sched.lock);
	srctl *p = t;
	p = sr_ctladd(p, "threads",             SR_CTLINT,          &c->threads,              NULL);
	p = sr_ctladd(p, "checkpoint_active",   SR_CTLINT|SR_CTLRO, &si->checkpoint_active,   NULL);
	p = sr_ctladd(p, "checkpoint_lsn",      SR_CTLU64|SR_CTLRO, &si->checkpoint_lsn,      NULL);
	p = sr_ctladd(p, "checkpoint_lsn_last", SR_CTLU64|SR_CTLRO, &si->checkpoint_lsn_last, NULL);
	p = sr_ctladd(p, "checkpoint",          SR_CTLTRIGGER,      NULL,                     so_schedulerctl_checkpoint);
	p = sr_ctladd(p, "run",                 SR_CTLTRIGGER,      NULL,                     so_schedulerctl_run);
	srlist *i;
	sr_listforeach(&o->sched.workers.list, i) {
		soworker *w = srcast(i, soworker, link);
		p = sr_ctladd(p, w->name, SR_CTLSUB, w, NULL);
	}
	p = sr_ctladd(p,  NULL, 0, NULL, NULL);
}

int so_schedulerctl_set(soobj *obj, char *path, va_list args)
{
	so *o = (so*)obj;
	soschedulerinfo si;
	srctl ctls[30];
	so_schedulerctl_prepare(&ctls[0], o, &o->ctl, &si);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&o->error, "%s", "bad control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	int type = match->type & ~SR_CTLRO;
	if (so_active(o) && (type != SR_CTLTRIGGER)) {
		sr_error(&o->error, "%s", "failed to set control path");
		sr_error_recoverable(&o->error);
		return -1;
	}
	rc = sr_ctlset(match, &o->a, o, args);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(&o->error);
		return -1;
	}
	return rc;
}

static void*
so_schedulerctl_workerget(so *o, soworker *w, char *path)
{
	char tracesz[128];
	char *trace;
	int tracelen = sr_tracecopy(&w->trace, tracesz, sizeof(tracesz));
	if (srlikely(tracelen == 0))
		trace = NULL;
	else
		trace = tracesz;
	srctl ctls[30];
	srctl *p = &ctls[0];
	p = sr_ctladd(p, "trace", SR_CTLSTRING|SR_CTLRO, trace, NULL);
	p = sr_ctladd(p,  NULL,   0,                     NULL,  NULL);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&o->error, "%s", "bad control path");
		sr_error_recoverable(&o->error);
		return NULL;
	}
	return so_ctlreturn(match, o);
}

static int
so_schedulerctl_workerdump(so *o, soworker *w, srbuf *dump)
{
	char tracesz[128];
	char *trace;
	int tracelen = sr_tracecopy(&w->trace, tracesz, sizeof(tracesz));
	if (srlikely(tracelen == 0))
		trace = NULL;
	else
		trace = tracesz;
	srctl ctls[30];
	srctl *p = &ctls[0];
	p = sr_ctladd(p, "trace", SR_CTLSTRING|SR_CTLRO, trace, NULL);
	p = sr_ctladd(p,  NULL,   0,                     NULL,  NULL);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "scheduler.%s.", w->name);
	int rc = sr_ctlserialize(&ctls[0], &o->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&o->error, "%s", "memory allocation failed");
		sr_error_recoverable(&o->error);
		return -1;
	}
	return 0;
}

void*
so_schedulerctl_get(soobj *obj, char *path, va_list args srunused)
{
	so *o = (so*)obj;
	soschedulerinfo si;
	srctl ctls[30];
	so_schedulerctl_prepare(&ctls[0], o, &o->ctl, &si);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc == 1 || rc == -1)) {
		sr_error(&o->error, "%s", "bad control path");
		sr_error_recoverable(&o->error);
		return NULL;
	}
	int type = match->type & ~SR_CTLRO;
	if (type == SR_CTLSUB) {
		return so_schedulerctl_workerget(o, match->v, path);
	}
	return so_ctlreturn(match, o);
}

int
so_schedulerctl_dump(soobj *obj, srbuf *dump)
{
	so *o = (so*)obj;
	soschedulerinfo si;
	srctl ctls[30];
	so_schedulerctl_prepare(&ctls[0], o, &o->ctl, &si);
	char prefix[64];
	snprintf(prefix, sizeof(prefix), "scheduler.");
	int rc = sr_ctlserialize(&ctls[0], &o->a, prefix, dump);
	if (srunlikely(rc == -1)) {
		sr_error(&o->error, "%s", "memory allocation failed");
		sr_error_recoverable(&o->error);
		return -1;
	}
	srlist *i;
	sr_listforeach(&o->sched.workers.list, i) {
		soworker *w = srcast(i, soworker, link);
		rc = so_schedulerctl_workerdump(o, w, dump);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}
