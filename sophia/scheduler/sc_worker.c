
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
#include <libsd.h>
#include <libsw.h>
#include <libsi.h>
#include <libsy.h>
#include <libsc.h>

static inline scworker*
sc_workernew(sr *r, int id)
{
	scworker *w = ss_malloc(r->a, sizeof(scworker));
	if (ssunlikely(w == NULL)) {
		sr_oom_malfunction(r->e);
		return NULL;
	}
	snprintf(w->name, sizeof(w->name), "%d", id);
	sd_cinit(&w->dc);
	ss_listinit(&w->link);
	ss_listinit(&w->linkidle);
	ss_traceinit(&w->trace);
	ss_trace(&w->trace, "%s", "init");
	return w;
}

static inline void
sc_workerfree(scworker *w, sr *r)
{
	sd_cfree(&w->dc, r);
	ss_tracefree(&w->trace);
	ss_free(r->a, w);
}

int sc_workerpool_init(scworkerpool *p)
{
	ss_spinlockinit(&p->lock);
	ss_listinit(&p->list);
	ss_listinit(&p->listidle);
	p->total = 0;
	p->idle = 0;
	return 0;
}

int sc_workerpool_free(scworkerpool *p, sr *r)
{
	sslist *i, *n;
	ss_listforeach_safe(&p->list, i, n) {
		scworker *w = sscast(i, scworker, link);
		sc_workerfree(w, r);
	}
	return 0;
}

int sc_workerpool_new(scworkerpool *p, sr *r)
{
	scworker *w = sc_workernew(r, p->total);
	if (ssunlikely(w == NULL))
		return -1;
	ss_listappend(&p->list, &w->link);
	ss_listappend(&p->listidle, &w->linkidle);
	p->total++;
	p->idle++;
	return 0;
}
