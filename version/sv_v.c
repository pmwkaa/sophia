
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
	register svv *vv = v->v;
	return sv_vvalue(vv);
}

static int
sv_vifvaluecopy(sv *v, char *dest)
{
	register svv *vv = v->v;
	memcpy(dest, sv_vvalue(vv), vv->valuesize);
	return 0;
}

static uint32_t
sv_vifvaluesize(sv *v) {
	return ((svv*)v->v)->valuesize;
}

static uint64_t
sv_vifoffset(sv *v) {
	(void)v;
	assert(0);
	return 0;
}

static char*
sv_vifraw(sv *v) {
	assert(0);
	(void)v;
	return NULL;
}

static uint32_t
sv_vifrawsize(sv *v) {
	assert(0);
	(void)v;
}

static void
sv_vifref(sv *v) {
	(void)v;
	assert(0);
}

static void
sv_vifunref(sv *v, sra *a) {
	(void)v;
	(void)a;
	assert(0);
}

svif sv_vif =
{
	.flags       = sv_vifflags,
	.lsn         = sv_viflsn,
	.lsnset      = sv_viflsnset,
	.key         = sv_vifkey,
	.keysize     = sv_vifkeysize,
	.value       = sv_vifvalue,
	.valuecopy   = sv_vifvaluecopy,
	.valuesize   = sv_vifvaluesize,
	.valueoffset = sv_vifoffset,
	.raw         = sv_vifraw,
	.rawsize     = sv_vifrawsize,
	.ref         = sv_vifref,
	.unref       = sv_vifunref
};
