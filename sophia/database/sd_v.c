
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
sd_vifflags(sv *v)
{
	return ((sdv*)v->v)->flags;
}

static uint64_t
sd_viflsn(sv *v)
{
	return ((sdv*)v->v)->lsn;
}

static char*
sd_vifpointer(sv *v)
{
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagepointer(&p, (sdv*)v->v);
}

static uint32_t
sd_viftimestamp(sv *v)
{
	return ((sdv*)v->v)->timestamp;
}

static uint32_t
sd_vifsize(sv *v)
{
	return ((sdv*)v->v)->size;
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

static char*
sd_vrawifpointer(sv *v)
{
	return (char*)v->v + sizeof(sdv);
}

svif sd_vrawif =
{
	.flags     = sd_vifflags,
	.lsn       = sd_viflsn,
	.lsnset    = NULL,
	.timestamp = sd_viftimestamp,
	.pointer   = sd_vrawifpointer,
	.size      = sd_vifsize
};
