
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
sd_vifpointer(sv *v)
{
	sdv *dv = v->v;
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pageof(&p, dv);
}

static uint32_t
sd_vifsize(sv *v) {
	return ((sdv*)v->v)->size;
}

svif sd_vif =
{
	.flags   = sd_vifflags,
	.lsn     = sd_viflsn,
	.lsnset  = NULL,
	.pointer = sd_vifpointer,
	.size    = sd_vifsize
};
