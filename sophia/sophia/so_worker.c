
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
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libse.h>
#include <libso.h>

int so_workersinit(soworkers *w)
{
	ss_listinit(&w->list);
	w->n = 0;
	return 0;
}

static inline int
so_workershutdown(soworker *w, sr *r)
{
	int rc = ss_threadjoin(&w->t);
	if (ssunlikely(rc == -1))
		sr_malfunction(r->e, "failed to join a thread: %s",
		               strerror(errno));
	sd_cfree(&w->dc, r);
	ss_tracefree(&w->trace);
	ss_free(r->a, w);
	return rc;
}

int so_workersshutdown(soworkers *w, sr *r)
{
	int rcret = 0;
	int rc;
	sslist *i, *n;
	ss_listforeach_safe(&w->list, i, n) {
		soworker *p = sscast(i, soworker, link);
		rc = so_workershutdown(p, r);
		if (ssunlikely(rc == -1))
			rcret = -1;
	}
	return rcret;
}

static inline soworker*
so_workernew(sr *r, int id, ssthreadf f, void *arg)
{
	soworker *p = ss_malloc(r->a, sizeof(soworker));
	if (ssunlikely(p == NULL)) {
		sr_oom_malfunction(r->e);
		return NULL;
	}
	snprintf(p->name, sizeof(p->name), "%d", id);
	p->arg = arg;
	sd_cinit(&p->dc);
	ss_listinit(&p->link);
	ss_traceinit(&p->trace);
	ss_trace(&p->trace, "%s", "init");
	int rc = ss_threadnew(&p->t, f, p);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "failed to create thread: %s",
		               strerror(errno));
		ss_free(r->a, p);
		return NULL;
	}
	return p;
}

int so_workersnew(soworkers *w, sr *r, int n, ssthreadf f, void *arg)
{
	int i = 0;
	int id = 0;
	while (i < n) {
		soworker *p = so_workernew(r, id, f, arg);
		if (ssunlikely(p == NULL))
			return -1;
		ss_listappend(&w->list, &p->link);
		w->n++;
		i++;
		id++;
	}
	return 0;
}
