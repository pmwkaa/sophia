
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

static uint8_t
sx_vifflags(sv *v) {
	return ((sxv*)v->v)->v->flags;
}

static uint64_t
sx_viflsn(sv *v) {
	return ((sxv*)v->v)->v->lsn;
}

static void
sx_viflsnset(sv *v, uint64_t lsn) {
	((sxv*)v->v)->v->lsn = lsn;
}

static uint32_t
sx_viftimestamp(sv *v) {
	return ((sxv*)v->v)->v->timestamp;
}

static char*
sx_vifpointer(sv *v) {
	return sv_vpointer(((sxv*)v->v)->v);
}

static uint32_t
sx_vifsize(sv *v) {
	return ((sxv*)v->v)->v->size;
}

svif sx_vif =
{
	.flags     = sx_vifflags,
	.lsn       = sx_viflsn,
	.lsnset    = sx_viflsnset,
	.timestamp = sx_viftimestamp,
	.pointer   = sx_vifpointer,
	.size      = sx_vifsize
};
