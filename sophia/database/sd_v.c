
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
	sdv *dv = v->v;
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagelsnof(&p, dv);
}

static char*
sd_vifpointer(sv *v)
{
	sdv *dv = v->v;
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	uint64_t size, lsn;
	return sd_pagemetaof(&p, dv, &size, &lsn);
}

static uint32_t
sd_vifsize(sv *v) {
	sdv *dv = v->v;
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	uint64_t size, lsn;
	sd_pagemetaof(&p, dv, &size, &lsn);
	return size;
}

svif sd_vif =
{
	.flags   = sd_vifflags,
	.lsn     = sd_viflsn,
	.lsnset  = NULL,
	.pointer = sd_vifpointer,
	.size    = sd_vifsize
};
