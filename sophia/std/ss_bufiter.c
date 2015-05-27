
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

ssiterif ss_bufiter =
{
	.close   = ss_bufiter_close,
	.has     = ss_bufiter_has,
	.of      = ss_bufiter_of,
	.next    = ss_bufiter_next
};

ssiterif ss_bufiterref =
{
	.close   = ss_bufiterref_close,
	.has     = ss_bufiterref_has,
	.of      = ss_bufiterref_of,
	.next    = ss_bufiterref_next
};
