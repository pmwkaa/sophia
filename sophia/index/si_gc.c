
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

uint32_t si_gcv(sr *r, svv *v)
{
	uint32_t size = sv_vsize(v);
	sl *log = (sl*)v->log;
	if (sv_vunref(r, v)) {
		if (log)
			ss_gcsweep(&log->gc, 1);
		return size;
	}
	return 0;
}

uint32_t si_gcref(sr *r, svref *gc)
{
	uint32_t used = 0;
	svref *v = gc;
	while (v) {
		svref *n = v->next;
		uint32_t size = sv_vsize(v->v);
		if (si_gcv(r, v->v))
			used += size;
		ss_free(r->aref, v);
		v = n;
	}
	return used;
}
