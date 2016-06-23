
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
sv_v_kv(void)
{
	uint32_t key = 123;
	char value[] = "hello";

	sfv pv[8];
	memset(pv, 0, sizeof(pv));
	pv[0].pointer = (char*)&key;
	pv[0].size = sizeof(key);
	pv[1].pointer = value;
	pv[1].size = sizeof(value);

	svv *v = sv_vbuild(&st_r.r, pv);
	t( v != NULL );

	t( sf_flags(st_r.r.scheme, sv_vpointer(v)) == 0 );

	t( *(uint32_t*)sf_field(&st_r.scheme, 0, sv_vpointer(v), &st_r.size) == key );
	t( sf_fieldsize(&st_r.scheme, 0, sv_vpointer(v)) == sizeof(key) );

	sv_vunref(&st_r.r, v);
}

stgroup *sv_v_group(void)
{
	stgroup *group = st_group("svv");
	st_groupadd(group, st_test("kv", sv_v_kv));
	return group;
}
