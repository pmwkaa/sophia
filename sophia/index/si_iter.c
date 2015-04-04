
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

sriterif si_iter =
{
	.close = si_iter_close,
	.has   = si_iter_has,
	.of    = si_iter_of,
	.next  = si_iter_next
};
