
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
#include <libso.h>
#include <libsx.h>

static char*
sx_vifpointer(sv *v) {
	return sv_vpointer(((sxv*)v->v)->v);
}

svif sx_vif =
{
	.pointer = sx_vifpointer
};
