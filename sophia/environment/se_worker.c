
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
se_workernew(sr *r, int id)
{
	seworker *w = ss_malloc(r->a, sizeof(seworker));
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
se_workerfree(seworker *w, sr *r)
{
	sd_cfree(&w->dc, r);
	ss_tracefree(&w->trace);
	ss_free(r->a, w);
}

int se_workerpool_init(seworkerpool *p)
{
	ss_spinlockinit(&p->lock);
	ss_listinit(&p->list);
	ss_listinit(&p->listidle);
	p->total = 0;
	p->idle = 0;
	return 0;
}

int se_workerpool_free(seworkerpool *p, sr *r)
{
	sslist *i, *n;
	ss_listforeach_safe(&p->list, i, n) {
		seworker *w = sscast(i, seworker, link);
		se_workerfree(w, r);
	}
	return 0;
}

int se_workerpool_new(seworkerpool *p, sr *r)
{
	seworker *w = se_workernew(r, p->total);
	if (ssunlikely(w == NULL))
		return -1;
	ss_listappend(&p->list, &w->link);
	ss_listappend(&p->listidle, &w->linkidle);
	p->total++;
	p->idle++;
	return 0;
}
