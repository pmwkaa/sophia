
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

static uint64_t
sv_viflsn(sv *v) {
	return ((svv*)v->v)->lsn;
}

static void
sv_viflsnset(sv *v, uint64_t lsn) {
	((svv*)v->v)->lsn = lsn;
}

static char*
sv_vifpointer(sv *v) {
	return sv_vpointer(((svv*)v->v));
}

static uint32_t
sv_vifsize(sv *v) {
	return ((svv*)v->v)->size;
}

svif sv_vif =
{
	.flags   = sv_vifflags,
	.lsn     = sv_viflsn,
	.lsnset  = sv_viflsnset,
	.pointer = sv_vifpointer,
	.size    = sv_vifsize
};
