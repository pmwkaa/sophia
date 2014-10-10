
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
	t( sp_set(c, "db.test") == 0 );

	void *o = sp_get(c, "db.test.error");
	t( o != NULL );
	char *value = sp_get(o, "value", NULL);
	t( value == NULL );
	sp_destroy(o);

	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_error(db) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *error_group(void)
{
	stgroup *group = st_group("error");
	st_groupadd(group, st_test("test0", error_test0));
	return group;
}
