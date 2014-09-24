
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>

static uint8_t
sl_vifflags(sv *v) {
	return ((slv*)v->v)->flags;
}

static uint64_t
sl_viflsn(sv *v) {
	return ((slv*)v->v)->lsn;
}

static void
sl_viflsnset(sv *v, uint64_t lsn) {
	(void)v;
	(void)lsn;
	assert(0);
}

static char*
sl_vifkey(sv *v) {
	return (char*)v->v + sizeof(slv);
}

static uint16_t
sl_vifkeysize(sv *v) {
	return ((slv*)v->v)->keysize;
}

static char*
sl_vifvalue(sv *v)
{
	return (char*)v->v + sizeof(slv) +
	       ((slv*)v->v)->keysize;
}

static int
sl_vifvaluecopy(sv *v, char *dest)
{
	void *ptr = (char*)v->v + sizeof(slv) +
	            ((slv*)v->v)->keysize;
	memcpy(dest, ptr, ((slv*)v->v)->valuesize);
	return 0;
}

static uint32_t
sl_vifvaluesize(sv *v) {
	return ((slv*)v->v)->valuesize;
}

static uint64_t
sl_vifoffset(sv *v) {
	(void)v;
	assert(0);
	return 0;
}

static char*
sl_vifraw(sv *v) {
	(void)v;
	assert(0);
	return NULL;
}

static uint32_t
sl_vifrawsize(sv *v) {
	(void)v;
	assert(0);
	return 0;
}

static void
sl_vifref(sv *v) {
	(void)v;
}

static void
sl_vifunref(sv *v, sra *a) {
	(void)v;
	(void)a;
}

svif sl_vif =
{
	.flags       = sl_vifflags,
	.lsn         = sl_viflsn,
	.lsnset      = sl_viflsnset,
	.key         = sl_vifkey,
	.keysize     = sl_vifkeysize,
	.value       = sl_vifvalue,
	.valuecopy   = sl_vifvaluecopy,
	.valuesize   = sl_vifvaluesize,
	.valueoffset = sl_vifoffset,
	.raw         = sl_vifraw,
	.rawsize     = sl_vifrawsize,
	.ref         = sl_vifref,
	.unref       = sl_vifunref
};
