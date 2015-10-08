
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

uint32_t si_gcv(sr *r, svv *gc)
{
	uint32_t used = 0;
	svv *v = gc;
	while (v) {
		used += sv_vsize(v);
		svv *n = v->next;
		sl *log = (sl*)v->log;
		if (log)
			ss_gcsweep(&log->gc, 1);
		sv_vfree(r, v);
		v = n;
	}
	return used;
}
