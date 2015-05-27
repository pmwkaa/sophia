
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

ssiterif sd_schemeiter =
{
	.close   = sd_schemeiter_close,
	.has     = sd_schemeiter_has,
	.of      = sd_schemeiter_of,
	.next    = sd_schemeiter_next
};
