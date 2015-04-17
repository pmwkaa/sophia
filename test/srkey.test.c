
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>

stgroup *srkey_group(void)
{
	stgroup *group = st_group("srkey");
	return group;
}
