
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

void si_gcv(sr *r, svv *v)
{
	sl *log = (sl*)v->log;
	if (sv_vunref(r, v)) {
		if (log)
			ss_gcsweep(&log->gc, 1);
	}
}

void si_gcref(sr *r, svref *gc)
{
	svref *v = gc;
	while (v) {
		svref *n = v->next;
		si_gcv(r, v->v);
		ss_free(r->aref, v);
		v = n;
	}
}
