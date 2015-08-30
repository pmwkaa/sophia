
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

static inline seworker*
se_workernew(sr *r, int id, ssthreadf f, void *arg)
{
	seworker *w = ss_malloc(r->a, sizeof(seworker));
	if (ssunlikely(w == NULL)) {
		sr_oom_malfunction(r->e);
		return NULL;
	}
	snprintf(w->name, sizeof(w->name), "%d", id);
	w->arg = arg;
	sd_cinit(&w->dc, r);
	ss_listinit(&w->link);
	ss_traceinit(&w->trace);
	ss_trace(&w->trace, "%s", "init");
	int rc = ss_threadnew(&w->t, f, w);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "failed to create thread: %s",
		               strerror(errno));
		ss_free(r->a, w);
		return NULL;
	}
	return w;
}

static inline int
se_workershutdown(seworker *w, sr *r)
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

int se_workerpool_init(seworkerpool *w)
{
	ss_listinit(&w->list);
	w->n = 0;
	return 0;
}

int se_workerpool_shutdown(seworkerpool *p, sr *r)
{
	int rcret = 0;
	int rc;
	sslist *i, *n;
	ss_listforeach_safe(&p->list, i, n) {
		seworker *w = sscast(i, seworker, link);
		rc = se_workershutdown(w, r);
		if (ssunlikely(rc == -1))
			rcret = -1;
	}
	return rcret;
}

int se_workerpool_new(seworkerpool *p, sr *r, int n, ssthreadf f, void *arg)
{
	int i = 0;
	int id = 0;
	while (i < n) {
		seworker *w = se_workernew(r, id, f, arg);
		if (ssunlikely(p == NULL))
			return -1;
		ss_listappend(&p->list, &w->link);
		p->n++;
		i++;
		id++;
	}
	return 0;
}
