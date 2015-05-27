
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

ssiterif sv_mergeiter =
{
	.close = sv_mergeiter_close,
	.has   = sv_mergeiter_has,
	.of    = sv_mergeiter_of,
	.next  = sv_mergeiter_next
};
