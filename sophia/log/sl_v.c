
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

static void
sl_vifflagsadd(sv *v, uint32_t flags) {
	((slv*)v->v)->flags |= flags;
}

static uint64_t
sl_viflsn(sv *v) {
	return ((slv*)v->v)->lsn;
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

static uint32_t
sl_vifvaluesize(sv *v) {
	return ((slv*)v->v)->valuesize;
}

svif sl_vif =
{
	.flags       = sl_vifflags,
	.flagsadd    = sl_vifflagsadd,
	.lsn         = sl_viflsn,
	.lsnset      = NULL,
	.key         = sl_vifkey,
	.keysize     = sl_vifkeysize,
	.value       = sl_vifvalue,
	.valuesize   = sl_vifvaluesize
};
