
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
#include <sophia.h>

int so_workersinit(soworkers *w)
{
	sr_listinit(&w->list);
	w->n = 0;
	return 0;
}

static inline int
so_workershutdown(soworker *w, sr *r)
{
	sr_threadwakeup(&w->t);
	int rc = sr_threadjoin(&w->t);
	if (srunlikely(rc == -1))
		sr_error(r->e, "failed to join a thread: %s",
		         strerror(errno));
	sd_cfree(&w->dc, r);
	sr_tracefree(&w->trace);
	sr_free(r->a, w);
	return rc;
}

int so_workersshutdown(soworkers *w, sr *r)
{
	int rcret = 0;
	int rc;
	srlist *i, *n;
	sr_listforeach_safe(&w->list, i, n) {
		soworker *p = srcast(i, soworker, link);
		rc = so_workershutdown(p, r);
		if (srunlikely(rc == -1))
			rcret = -1;
	}
	return rcret;
}

static inline soworker*
so_workernew(sr *r, int id, srthreadf f, void *arg)
{
	soworker *p = sr_malloc(r->a, sizeof(soworker));
	if (srunlikely(p == NULL)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return NULL;
	}
	snprintf(p->name, sizeof(p->name), "%d", id);
	p->arg = arg;
	sd_cinit(&p->dc, r);
	sr_listinit(&p->link);
	sr_traceinit(&p->trace);
	sr_trace(&p->trace, "%s", "init");
	int rc = sr_threadnew(&p->t, f, p);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "failed to create thread: %s",
		         strerror(errno));
		sr_free(r->a, p);
		return NULL;
	}
	return p;
}

int so_workersnew(soworkers *w, sr *r, int n, srthreadf f, void *arg)
{
	int i = 0;
	int id = 0;
	while (i < n) {
		soworker *p = so_workernew(r, id, f, arg);
		if (srunlikely(p == NULL))
			return -1;
		sr_listappend(&w->list, &p->link);
		w->n++;
		i++;
		id++;
	}
	return 0;
}
