
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
#include <libsv.h>
#include <libst.h>

static void
svv_kv(stc *cx)
{
	ssa a;
	ss_aopen(&a, &ss_stda);

	srscheme cmp;
	sr_schemeinit(&cmp);
	srkey *part = sr_schemeadd(&cmp, &a);
	t( sr_keysetname(part, &a, "key") == 0 );
	t( sr_keyset(part, &a, "u32") == 0 );
	sr r;
	sr_init(&r, NULL, &a, NULL, SF_KV, SF_SRAW, &cmp, NULL, NULL, NULL);

	uint32_t key = 123;
	uint32_t value = 321;

	sfv pv;
	pv.key = (char*)&key;
	pv.r.size = sizeof(key);
	pv.r.offset = 0;

	svv *vv = sv_vbuild(&r, &pv, 1, (char*)&value, sizeof(value));
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

	t( *(uint32_t*)sf_value(SF_KV, sv_pointer(&v), cmp.count) == value );
	t( sf_valuesize(SF_KV, sv_pointer(&v), sv_size(&v), cmp.count) == sizeof(value) );

	sv_vfree(&a, vv);
	sr_schemefree(&cmp, &a);
}

stgroup *svv_group(void)
{
	stgroup *group = st_group("svv");
	st_groupadd(group, st_test("kv", svv_kv));
	return group;
}
