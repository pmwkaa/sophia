
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
sv_upsertvifflags(sv *v) {
	svupsertnode *n = v->v;
	return n->flags;
}

static char*
sv_upsertvifpointer(sv *v) {
	svupsertnode *n = v->v;
	return n->buf.s;
}

static uint32_t
sv_upsertvifsize(sv *v) {
	svupsertnode *n = v->v;
	return ss_bufused(&n->buf);
}

svif sv_upsertvif =
{
	.flags   = sv_upsertvifflags,
	.pointer = sv_upsertvifpointer,
	.size    = sv_upsertvifsize
};
