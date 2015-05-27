
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

ssiterif sd_iter =
{
	.close   = sd_iter_close,
	.has     = sd_iter_has,
	.of      = sd_iter_of,
	.next    = sd_iter_next
};
