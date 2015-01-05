
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
	sr_allocopen(&a, &sr_astd);

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
	svinit(&lv, &sv_localif, &l, NULL);
	svv *vv = sv_valloc(&a, &lv);
	t( vv != NULL );
	sv v;
	svinit(&v, &sv_vif, vv, NULL);
	t( svflags(&v) == l.flags );
	svflagsadd(&v, SVDUP);
	t( svflags(&v) == (l.flags|SVDUP) );
	t( svlsn(&v) == l.lsn );
	svlsnset(&v, 8);
	t( svlsn(&v) == 8 );
	t( *(uint32_t*)svkey(&v) == key );
	t( svkeysize(&v) == l.keysize );
	t( *(uint32_t*)svvalue(&v) == value );
	t( svvaluesize(&v) == l.valuesize );
	sv_vfree(&a, vv);
}

stgroup *svv_group(void)
{
	stgroup *group = st_group("svv");
	st_groupadd(group, st_test("test", svv_test));
	return group;
}
