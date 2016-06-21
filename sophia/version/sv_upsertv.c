
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

svif sv_upsertvif =
{
	.flags   = sv_upsertvifflags,
	.pointer = sv_upsertvifpointer
};
