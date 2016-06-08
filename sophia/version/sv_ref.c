
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

static uint8_t
sv_refifflags(sv *v) {
	svref *ref = (svref*)v->v;
	return ((svv*)ref->v)->flags | ref->flags;
}

static uint64_t
sv_refiflsn(sv *v) {
	return ((svv*)((svref*)v->v)->v)->lsn;
}

static void
sv_refiflsnset(sv *v, uint64_t lsn) {
	((svv*)((svref*)v->v)->v)->lsn = lsn;
}

static char*
sv_refifpointer(sv *v) {
	return sv_vpointer(((svv*)((svref*)v->v)->v));
}

static uint32_t
sv_refifsize(sv *v) {
	return ((svv*)((svref*)v->v)->v)->size;
}

svif sv_refif =
{
	.flags   = sv_refifflags,
	.lsn     = sv_refiflsn,
	.lsnset  = sv_refiflsnset,
	.pointer = sv_refifpointer,
	.size    = sv_refifsize
};
