
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
#include <libsd.h>
#include <libst.h>

static void
method_unsupported(void)
{
	void *env = sp_env();
	t( env != NULL );
	void *o = sp_object(env);
	t(o == NULL);
	char *value = sp_getstring(o, "sophia.error", 0);
	t( value != NULL );
	t( strstr(value, "unsupported") != NULL );
	free(value);
	sp_destroy(env);
}

stgroup *method_group(void)
{
	stgroup *group = st_group("method");
	(void)method_unsupported;
	return group;
}
