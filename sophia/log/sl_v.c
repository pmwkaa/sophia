
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
#include <libsl.h>

static char*
sl_vifpointer(sv *v) {
	return (char*)v->v + sizeof(slv);
}

svif sl_vif =
{
	.pointer = sl_vifpointer
};
