
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

sriterif sd_pageiter =
{
	.close   = sd_pageiter_close,
	.has     = sd_pageiter_has,
	.of      = sd_pageiter_of,
	.next    = sd_pageiter_next
};

sriterif sd_pageiterraw =
{
	.close = sd_pageiterraw_close,
	.has   = sd_pageiterraw_has,
	.of    = sd_pageiterraw_of,
	.next  = sd_pageiterraw_next,
};
