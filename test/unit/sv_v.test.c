
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

	sfv pv[2];
	pv[0].pointer = (char*)&key;
	pv[0].size = sizeof(key);
	pv[1].pointer = value;
	pv[1].size = sizeof(value);

	svv *vv = sv_vbuild(&st_r.r, pv);
	t( vv != NULL );
	vv->flags = 0;
	vv->lsn = 10;
	sv v;
	sv_init(&v, &sv_vif, vv, NULL);

	t( sv_flags(&v) == 0 );
	t( sv_lsn(&v) == 10 );
	sv_lsnset(&v, 8);
	t( sv_lsn(&v) == 8 );

	t( *(uint32_t*)sf_field(&st_r.scheme, 0, sv_pointer(&v)) == key );
	t( sf_fieldsize(&st_r.scheme, 0, sv_pointer(&v)) == sizeof(key) );

	sv_vunref(&st_r.r, vv);
}

stgroup *sv_v_group(void)
{
	stgroup *group = st_group("svv");
	st_groupadd(group, st_test("kv", sv_v_kv));
	return group;
}
