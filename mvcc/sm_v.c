
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>

static uint8_t
sm_vifflags(sv *v) {
	return ((smv*)v->v)->v->flags;
}

static uint64_t
sm_viflsn(sv *v) {
	return ((smv*)v->v)->v->lsn;
}

static void
sm_viflsnset(sv *v, uint64_t lsn) {
	((smv*)v->v)->v->lsn = lsn;
}

static char*
sm_vifkey(sv *v) {
	return sv_vkey(((smv*)v->v)->v);
}

static uint16_t
sm_vifkeysize(sv *v) {
	return ((smv*)v->v)->v->keysize;
}

static char*
sm_vifvalue(sv *v)
{
	smv *vv = v->v;
	if (vv->v->valuesize == 0)
		return NULL;
	return sv_vvalue(vv->v);
}

static uint32_t
sm_vifvaluesize(sv *v) {
	return ((smv*)v->v)->v->valuesize;
}

svif sm_vif =
{
	.flags       = sm_vifflags,
	.lsn         = sm_viflsn,
	.lsnset      = sm_viflsnset,
	.key         = sm_vifkey,
	.keysize     = sm_vifkeysize,
	.value       = sm_vifvalue,
	.valuesize   = sm_vifvaluesize,
	.valueoffset = NULL,
	.raw         = NULL,
	.rawsize     = NULL,
	.ref         = NULL,
	.unref       = NULL
};
