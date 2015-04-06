
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
svv_test(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);

	uint32_t key = 123;
	uint32_t value = 321;
	svlocal l;
	l.lsn         = 777;
	l.flags       = SVSET;
	l.key         = &key;
	l.keysize     = sizeof(key);
	l.value       = &value;
	l.valuesize   = sizeof(value);
	sv lv;
	sv_init(&lv, &sv_localif, &l, NULL);
	svv *vv = sv_valloc(&a, &lv);
	t( vv != NULL );
	sv v;
	sv_init(&v, &sv_vif, vv, NULL);
	t( sv_flags(&v) == l.flags );
	t( sv_lsn(&v) == l.lsn );
	sv_lsnset(&v, 8);
	t( sv_lsn(&v) == 8 );
	t( *(uint32_t*)sv_key(&v) == key );
	t( sv_keysize(&v) == l.keysize );
	t( *(uint32_t*)sv_value(&v) == value );
	t( sv_valuesize(&v) == l.valuesize );
	sv_vfree(&a, vv);
}

stgroup *svv_group(void)
{
	stgroup *group = st_group("svv");
	st_groupadd(group, st_test("test", svv_test));
	return group;
}
