
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
	sdv *dv = v->v;
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagevalue(&p, dv);
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

svif sd_vif =
{
	.flags       = sd_vifflags,
	.lsn         = sd_viflsn,
	.lsnset      = NULL,
	.key         = sd_vifkey,
	.keysize     = sd_vifkeysize,
	.value       = sd_vifvalue,
	.valuesize   = sd_vifvaluesize,
	.valueoffset = sd_vifoffset,
	.raw         = sd_vifraw,
	.rawsize     = sd_vifrawsize,
	.ref         = NULL,
	.unref       = NULL 
};
