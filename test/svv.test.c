
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libst.h>

static void
svv_kv(stc *cx)
{
	sra a;
	sr_aopen(&a, &sr_stda);

	srkey cmp;
	sr_keyinit(&cmp);
	srkeypart *part = sr_keyadd(&cmp, &a);
	t( sr_keypart_setname(part, &a, "key") == 0 );
	t( sr_keypart_set(part, &a, "u32") == 0 );
	sr r;
	sr_init(&r, NULL, &a, NULL, SR_FKV, &cmp, NULL, NULL, NULL);

	uint32_t key = 123;
	uint32_t value = 321;

	srformatv pv;
	pv.key = (char*)&key;
	pv.r.size = sizeof(key);
	pv.r.offset = 0;

	svv *vv = sv_vbuild(&r, &pv, 1, (char*)&value, sizeof(value));
	t( vv != NULL );
	vv->flags = SVSET;
	vv->lsn = 10;
	sv v;
	sv_init(&v, &sv_vif, vv, NULL);

	t( sv_flags(&v) == SVSET );
	t( sv_lsn(&v) == 10 );
	sv_lsnset(&v, 8);
	t( sv_lsn(&v) == 8 );

	t( *(uint32_t*)sr_formatkey(sv_pointer(&v), 0) == key );
	t( sr_formatkey_size(sv_pointer(&v), 0) == sizeof(key) );

	t( *(uint32_t*)sr_formatvalue(SR_FKV, &cmp, sv_pointer(&v)) == value );
	t( sr_formatvalue_size(SR_FKV, &cmp, sv_pointer(&v), sv_size(&v) ) == sizeof(value) );

	sv_vfree(&a, vv);
	sr_keyfree(&cmp, &a);
}

stgroup *svv_group(void)
{
	stgroup *group = st_group("svv");
	st_groupadd(group, st_test("kv", svv_kv));
	return group;
}
