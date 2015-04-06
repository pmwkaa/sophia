
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
sd_vifkey(sv *v)
{
	sdv *dv = v->v;
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagekey(&p, dv);
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

svif sd_vif =
{
	.flags     = sd_vifflags,
	.lsn       = sd_viflsn,
	.lsnset    = NULL,
	.key       = sd_vifkey,
	.keysize   = sd_vifkeysize,
	.value     = sd_vifvalue,
	.valuesize = sd_vifvaluesize
};
