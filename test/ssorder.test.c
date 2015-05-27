
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
#include <libst.h>

static void
ssorder_of(stc *cx)
{
	t( ss_orderof(">") == SS_GT );
	t( ss_orderof(">=") == SS_GTE );
	t( ss_orderof("<") == SS_LT );
	t( ss_orderof("<=") == SS_LTE );
}

static void
ssorder_name(stc *cx ssunused)
{
	t( strcmp(ss_ordername(SS_LT), "<") == 0 );
	t( strcmp(ss_ordername(SS_LTE), "<=") == 0 );
	t( strcmp(ss_ordername(SS_GT), ">") == 0 );
	t( strcmp(ss_ordername(SS_GTE), ">=") == 0 );
}

stgroup *ssorder_group(void)
{
	stgroup *group = st_group("ssorder");
	st_groupadd(group, st_test("of", ssorder_of));
	st_groupadd(group, st_test("name", ssorder_name));
	return group;
}
