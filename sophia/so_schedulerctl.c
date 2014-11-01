
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

static inline void
so_schedulerctl_prepare(srctl *t, so *o, soctl *c)
{
	srctl *p = t;
	p = sr_ctladd(p, "threads",        SR_CTLU32, &c->threads,        NULL);
	p = sr_ctladd(p, "node_size",      SR_CTLU32, &c->node_size,      NULL);
	p = sr_ctladd(p, "node_page_size", SR_CTLU32, &c->node_page_size, NULL);
	p = sr_ctladd(p, "node_branch_wm", SR_CTLU32, &c->node_branch_wm, NULL);
	p = sr_ctladd(p, "node_merge_wm",  SR_CTLU32, &c->node_merge_wm,  NULL);
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
	srctl ctls[30];
	so_schedulerctl_prepare(&ctls[0], o, &o->ctl);
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
	srctl ctls[30];
	so_schedulerctl_prepare(&ctls[0], o, &o->ctl);
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
	srctl ctls[30];
	so_schedulerctl_prepare(&ctls[0], o, &o->ctl);
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
