
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>

static void
srorder_of(stc *cx)
{
	t( sr_orderof(">") == SR_GT );
	t( sr_orderof(">=") == SR_GTE );
	t( sr_orderof("<") == SR_LT );
	t( sr_orderof("<=") == SR_LTE );
	t( sr_orderof("random") == SR_RANDOM );
}

static void
srorder_name(stc *cx srunused)
{
	t( strcmp(sr_ordername(SR_LT), "<") == 0 );
	t( strcmp(sr_ordername(SR_LTE), "<=") == 0 );
	t( strcmp(sr_ordername(SR_GT), ">") == 0 );
	t( strcmp(sr_ordername(SR_GTE), ">=") == 0 );
	t( strcmp(sr_ordername(SR_RANDOM), "random") == 0 );
}

stgroup *srorder_group(void)
{
	stgroup *group = st_group("srorder");
	st_groupadd(group, st_test("of", srorder_of));
	st_groupadd(group, st_test("name", srorder_name));
	return group;
}
