
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
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

static char*
sx_vifkey(sv *v) {
	return sv_vkey(((sxv*)v->v)->v);
}

static uint16_t
sx_vifkeysize(sv *v) {
	return ((sxv*)v->v)->v->keysize;
}

static char*
sx_vifvalue(sv *v)
{
	sxv *vv = v->v;
	if (vv->v->valuesize == 0)
		return NULL;
	return sv_vvalue(vv->v);
}

static uint32_t
sx_vifvaluesize(sv *v) {
	return ((sxv*)v->v)->v->valuesize;
}

svif sx_vif =
{
	.flags       = sx_vifflags,
	.lsn         = sx_viflsn,
	.lsnset      = sx_viflsnset,
	.key         = sx_vifkey,
	.keysize     = sx_vifkeysize,
	.value       = sx_vifvalue,
	.valuesize   = sx_vifvaluesize,
	.valueoffset = NULL,
	.raw         = NULL,
	.rawsize     = NULL,
	.ref         = NULL,
	.unref       = NULL
};
