
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
	uint32_t value = 321;

	sfv pv;
	pv.key = (char*)&key;
	pv.r.size = sizeof(key);
	pv.r.offset = 0;

	svv *vv = sv_vbuild(&st_r.r, &pv, 1, (char*)&value, sizeof(value));
	t( vv != NULL );
	vv->flags = 0;
	vv->lsn = 10;
	sv v;
	sv_init(&v, &sv_vif, vv, NULL);

	t( sv_flags(&v) == 0 );
	t( sv_lsn(&v) == 10 );
	sv_lsnset(&v, 8);
	t( sv_lsn(&v) == 8 );

	t( *(uint32_t*)sf_key(sv_pointer(&v), 0) == key );
	t( sf_keysize(sv_pointer(&v), 0) == sizeof(key) );

	t( *(uint32_t*)sf_value(SF_KV, sv_pointer(&v), st_r.scheme.count) == value );
	t( sf_valuesize(SF_KV, sv_pointer(&v), sv_size(&v), st_r.scheme.count) == sizeof(value) );

	sv_vfree(&st_r.a, vv);
}

stgroup *sv_v_group(void)
{
	stgroup *group = st_group("svv");
	st_groupadd(group, st_test("kv", sv_v_kv));
	return group;
}
