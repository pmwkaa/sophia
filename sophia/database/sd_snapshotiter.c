
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

ssiterif sd_snapshotiter =
{
	.close   = sd_snapshotiter_close,
	.has     = sd_snapshotiter_has,
	.of      = sd_snapshotiter_of,
	.next    = sd_snapshotiter_next
};
