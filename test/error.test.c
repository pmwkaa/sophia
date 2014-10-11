
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>
#include <sophia.h>

static void
error_test0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );

	void *o = sp_get(c, "sophia.error");
	t( o != NULL );
	char *value = sp_get(o, "value", NULL);
	t( value == NULL );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *error_group(void)
{
	stgroup *group = st_group("error");
	st_groupadd(group, st_test("test0", error_test0));
	return group;
}
