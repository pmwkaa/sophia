
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

sriterif sr_bufiter =
{
	.close   = sr_bufiter_close,
	.has     = sr_bufiter_has,
	.of      = sr_bufiter_of,
	.next    = sr_bufiter_next
};

sriterif sr_bufiterref =
{
	.close   = sr_bufiterref_close,
	.has     = sr_bufiterref_has,
	.of      = sr_bufiterref_of,
	.next    = sr_bufiterref_next
};
