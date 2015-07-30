
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
#include <libsi.h>

ssiterif si_read =
{
	.close = si_read_close,
	.has   = si_read_has,
	.of    = si_read_of,
	.next  = si_read_next
};
