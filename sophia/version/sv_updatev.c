
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
#include <libss.h>
#include <libsv.h>

static uint8_t
sv_updatevifflags(sv *v) {
	svupdatenode *n = v->v;
	return n->flags;
}

static uint64_t
sv_updateviflsn(sv *v) {
	svupdatenode *n = v->v;
	return n->lsn;
}

static void
sv_updateviflsnset(sv *v ssunused, uint64_t lsn ssunused) {
	assert(0);
}

static char*
sv_updatevifpointer(sv *v) {
	svupdatenode *n = v->v;
	return n->buf.s;
}

static uint32_t
sv_updatevifsize(sv *v) {
	svupdatenode *n = v->v;
	return ss_bufused(&n->buf);
}

svif sv_updatevif =
{
	.flags   = sv_updatevifflags,
	.lsn     = sv_updateviflsn,
	.lsnset  = sv_updateviflsnset,
	.pointer = sv_updatevifpointer,
	.size    = sv_updatevifsize
};
