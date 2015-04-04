
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

sriterif sv_indexiter =
{
	.close   = sv_indexiter_close,
	.has     = sv_indexiter_has,
	.of      = sv_indexiter_of,
	.next    = sv_indexiter_next
};

sriterif sv_indexiterraw =
{
	.close   = sv_indexiterraw_close,
	.has     = sv_indexiterraw_has,
	.of      = sv_indexiterraw_of,
	.next    = sv_indexiterraw_next
};
