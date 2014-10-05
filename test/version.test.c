
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
version_get(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );

	void *c = sp_ctl(env);
	t( c != NULL );

	void *o = sp_get(c, "sophia.version");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1.2") == 0 );
	sp_destroy(o);

	o = sp_get(c, "sophia.version_major");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	o = sp_get(c, "sophia.version_minor");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "2") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *version_group(void)
{
	stgroup *group = st_group("version");
	st_groupadd(group, st_test("get", version_get));
	return group;
}
