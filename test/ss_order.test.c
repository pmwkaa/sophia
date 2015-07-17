
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libst.h>

static void
ss_order_of(void)
{
	t( ss_orderof(">", 1) == SS_GT );
	t( ss_orderof(">=", 2) == SS_GTE );
	t( ss_orderof("<", 1) == SS_LT );
	t( ss_orderof("<=", 2) == SS_LTE );
}

static void
ss_order_name(void)
{
	t( strcmp(ss_ordername(SS_LT), "<") == 0 );
	t( strcmp(ss_ordername(SS_LTE), "<=") == 0 );
	t( strcmp(ss_ordername(SS_GT), ">") == 0 );
	t( strcmp(ss_ordername(SS_GTE), ">=") == 0 );
}

stgroup *ss_order_group(void)
{
	stgroup *group = st_group("ssorder");
	st_groupadd(group, st_test("of", ss_order_of));
	st_groupadd(group, st_test("name", ss_order_name));
	return group;
}
