
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

static uint8_t
sv_localifflags(sv *v) {
	return ((svlocal*)v->v)->flags;
}

static void
sv_localifflagsadd(sv *v, uint32_t flags) {
	((svlocal*)v->v)->flags |= flags;
}

static uint64_t
sv_localiflsn(sv *v) {
	return ((svlocal*)v->v)->lsn;
}

static void
sv_localiflsnset(sv *v, uint64_t lsn) {
	((svlocal*)v->v)->lsn = lsn;
}

static char*
sv_localifkey(sv *v) {
	return ((svlocal*)v->v)->key;
}

static uint16_t
sv_localifkeysize(sv *v) {
	return ((svlocal*)v->v)->keysize;
}

static char*
sv_localifvalue(sv *v)
{
	svlocal *lv = v->v;
	return lv->value;
}

static uint32_t
sv_localifvaluesize(sv *v) {
	return ((svlocal*)v->v)->valuesize;
}

svif sv_localif =
{
	.flags       = sv_localifflags,
	.flagsadd    = sv_localifflagsadd,
	.lsn         = sv_localiflsn,
	.lsnset      = sv_localiflsnset,
	.key         = sv_localifkey,
	.keysize     = sv_localifkeysize,
	.value       = sv_localifvalue,
	.valuesize   = sv_localifvaluesize
};
