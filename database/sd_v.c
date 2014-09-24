
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>

static uint8_t
sd_vifflags(sv *v) {
	return ((sdv*)v->v)->flags;
}

static uint64_t
sd_viflsn(sv *v) {
	return ((sdv*)v->v)->lsn;
}

static void
sd_viflsnset(sv *v, uint64_t lsn) {
	(void)v;
	(void)lsn;
	assert(0);
}

static char*
sd_vifkey(sv *v) {
	return ((sdv*)v->v)->key;
}

static uint16_t
sd_vifkeysize(sv *v) {
	return ((sdv*)v->v)->keysize;
}

static char*
sd_vifvalue(sv *v)
{
	(void)v;
	assert(0);
	return NULL;
}

static int
sd_vifvaluecopy(sv *v, char *dest)
{
	sdv *dv = v->v;
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	memcpy(dest, sd_pagevalue(&p, dv), dv->valuesize);
	return 0;
}

static uint32_t
sd_vifvaluesize(sv *v) {
	return ((sdv*)v->v)->valuesize;
}

static uint64_t
sd_vifoffset(sv *v) {
	return ((sdv*)v->v)->valueoffset;
}

static char*
sd_vifraw(sv *v) {
	return v->v;
}

static uint32_t
sd_vifrawsize(sv *v) {
	return sizeof(sdv) + ((sdv*)v->v)->keysize;
}

static void
sd_vifref(sv *v) {
	(void)v;
}

static void
sd_vifunref(sv *v, sra *a) {
	(void)v;
	(void)a;
}

svif sd_vif =
{
	.flags       = sd_vifflags,
	.lsn         = sd_viflsn,
	.lsnset      = sd_viflsnset,
	.key         = sd_vifkey,
	.keysize     = sd_vifkeysize,
	.value       = sd_vifvalue,
	.valuecopy   = sd_vifvaluecopy,
	.valuesize   = sd_vifvaluesize,
	.valueoffset = sd_vifoffset,
	.raw         = sd_vifraw,
	.rawsize     = sd_vifrawsize,
	.ref         = sd_vifref,
	.unref       = sd_vifunref
};
