
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
	ptr += ss_leb128skip(ptr);
	ptr += ss_leb128skip(ptr);
	if (sv_isflags(((sdv*)v->v)->flags, SVTIMESTAMP))
		ptr += ss_leb128skip(ptr);
	return ptr;
}

static uint32_t
sd_viftimestamp(sv *v) {
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagetimestampof(&p, (sdv*)v->v);
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
	.flags     = sd_vifflags,
	.lsn       = sd_viflsn,
	.lsnset    = NULL,
	.timestamp = sd_viftimestamp,
	.pointer   = sd_vifpointer,
	.size      = sd_vifsize
};

static uint64_t
sd_vrawiflsn(sv *v) {
	sdv *dv = v->v;
	char *ptr = (char*)dv + sizeof(sdv);
	ptr += ss_leb128skip(ptr);
	uint64_t val;
	ss_leb128read(ptr, &val);
	return val;
}

static uint32_t
sd_vrawiftimestamp(sv *v) {
	sdv *dv = v->v;
	if (! sv_isflags(dv->flags, SVTIMESTAMP))
		return UINT32_MAX;
	char *ptr = (char*)dv + sizeof(sdv);
	ptr += ss_leb128skip(ptr);
	ptr += ss_leb128skip(ptr);
	uint64_t ts;
	ss_leb128read(ptr, &ts);
	return ts;
}

static char*
sd_vrawifpointer(sv *v)
{
	sdv *dv = v->v;
	char *ptr = (char*)dv + sizeof(sdv);
	ptr += ss_leb128skip(ptr);
	ptr += ss_leb128skip(ptr);
	if (sv_isflags(dv->flags, SVTIMESTAMP))
		ptr += ss_leb128skip(ptr);
	return ptr;
}

static uint32_t
sd_vrawifsize(sv *v) {
	sdv *dv = v->v;
	uint64_t val;
	ss_leb128read((char*)dv + sizeof(sdv), &val);
	return val;
}

svif sd_vrawif =
{
	.flags     = sd_vifflags,
	.lsn       = sd_vrawiflsn,
	.lsnset    = NULL,
	.timestamp = sd_vrawiftimestamp,
	.pointer   = sd_vrawifpointer,
	.size      = sd_vrawifsize
};
