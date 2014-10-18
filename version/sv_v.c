
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

static uint8_t
sv_vifflags(sv *v) {
	return ((svv*)v->v)->flags;
}

static uint64_t
sv_viflsn(sv *v) {
	return ((svv*)v->v)->id.lsn;
}

static void
sv_viflsnset(sv *v, uint64_t lsn) {
	((svv*)v->v)->id.lsn = lsn;
}

static char*
sv_vifkey(sv *v) {
	return sv_vkey(((svv*)v->v));
}

static uint16_t
sv_vifkeysize(sv *v) {
	return ((svv*)v->v)->keysize;
}

static char*
sv_vifvalue(sv *v)
{
	svv *vv = v->v;
	if (vv->valuesize == 0)
		return NULL;
	return sv_vvalue(vv);
}

static uint32_t
sv_vifvaluesize(sv *v) {
	return ((svv*)v->v)->valuesize;
}

svif sv_vif =
{
	.flags       = sv_vifflags,
	.lsn         = sv_viflsn,
	.lsnset      = sv_viflsnset,
	.key         = sv_vifkey,
	.keysize     = sv_vifkeysize,
	.value       = sv_vifvalue,
	.valuesize   = sv_vifvaluesize,
	.valueoffset = NULL,
	.raw         = NULL,
	.rawsize     = NULL,
	.ref         = NULL,
	.unref       = NULL
};
