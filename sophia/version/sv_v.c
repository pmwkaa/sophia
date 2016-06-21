
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

static char*
sv_vifpointer(sv *v) {
	return sv_vpointer(((svv*)v->v));
}

svif sv_vif =
{
	.pointer = sv_vifpointer
};
