
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
method_unsupported(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *o = sp_object(env);
	t(o == NULL);
	void *c = sp_ctl(env);
	o = sp_get(c, "sophia.error");
	t( o != NULL );
	char *value = sp_get(o, "value", NULL);
	t( value != NULL );
	t( strstr(value, "unsupported") != NULL );
	sp_destroy(o);
	sp_destroy(env);
}

stgroup *method_group(void)
{
	stgroup *group = st_group("method");
	st_groupadd(group, st_test("unsupported", method_unsupported));
	return group;
}
