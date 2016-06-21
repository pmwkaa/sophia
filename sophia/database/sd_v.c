
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

static char*
sd_vifpointer(sv *v)
{
	sdpage p = {
		.h = (sdpageheader*)v->arg
	};
	return sd_pagepointer(&p, (sdv*)v->v);
}

static char*
sd_vrawifpointer(sv *v)
{
	return (char*)v->v + sizeof(sdv);
}

svif sd_vif =
{
	.pointer = sd_vifpointer
};

svif sd_vrawif =
{
	.pointer = sd_vrawifpointer
};
