
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
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagelsnof(&p, (sdv*)v->v);
}

static char*
sd_vifpointer(sv *v)
{
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	char *ptr = sd_pagepointer(&p, (sdv*)v->v);
	ptr += sr_leb128skip(ptr);
	ptr += sr_leb128skip(ptr);
	return ptr;
}

static uint32_t
sd_vifsize(sv *v) {
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagesizeof(&p, (sdv*)v->v);
}

svif sd_vif =
{
	.flags   = sd_vifflags,
	.lsn     = sd_viflsn,
	.lsnset  = NULL,
	.pointer = sd_vifpointer,
	.size    = sd_vifsize
};

static uint64_t
sd_vrawiflsn(sv *v) {
	sdv *dv = v->v;
	char *ptr = (char*)dv + sizeof(sdv);
	ptr += sr_leb128skip(ptr);
	uint64_t val;
	sr_leb128read(ptr, &val);
	return val;
}

static char*
sd_vrawifpointer(sv *v)
{
	sdv *dv = v->v;
	char *ptr = (char*)dv + sizeof(sdv);
	ptr += sr_leb128skip(ptr);
	ptr += sr_leb128skip(ptr);
	return ptr;
}

static uint32_t
sd_vrawifsize(sv *v) {
	sdv *dv = v->v;
	uint64_t val;
	sr_leb128read((char*)dv + sizeof(sdv), &val);
	return val;
}

svif sd_vrawif =
{
	.flags   = sd_vifflags,
	.lsn     = sd_vrawiflsn,
	.lsnset  = NULL,
	.pointer = sd_vrawifpointer,
	.size    = sd_vrawifsize
};
