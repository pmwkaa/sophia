
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

ssiterif sd_read =
{
	.close = sd_read_close,
	.has   = sd_read_has,
	.of    = sd_read_of,
	.next  = sd_read_next
};
