
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

static uint8_t
sv_vifflags(sv *v) {
	return ((svv*)v->v)->flags;
}

static char*
sv_vifpointer(sv *v) {
	return sv_vpointer(((svv*)v->v));
}

svif sv_vif =
{
	.flags   = sv_vifflags,
	.pointer = sv_vifpointer
};
