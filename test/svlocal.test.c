
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
svlocal_test(stc *cx srunused)
{
	uint32_t key = 123;
	uint32_t value = 321;
	svlocal l;
	l.lsn         = 777;
	l.flags       = SVSET;
	l.key         = &key;
	l.keysize     = sizeof(key);
	l.value       = &value;
	l.valuesize   = sizeof(value);
	sv v;
	sv_init(&v, &sv_localif, &l, NULL);
	t( sv_flags(&v) == l.flags );
	sv_flagsadd(&v, SVDUP);
	t( sv_flags(&v) == (l.flags|SVDUP) );
	t( sv_lsn(&v) == l.lsn );
	sv_lsnset(&v, 8);
	t( sv_lsn(&v) == 8 );
	t( sv_key(&v) == l.key );
	t( sv_keysize(&v) == l.keysize );
	t( sv_value(&v) == l.value );
	t( sv_valuesize(&v) == l.valuesize );
}

stgroup *svlocal_group(void)
{
	stgroup *group = st_group("svlocal");
	st_groupadd(group, st_test("test", svlocal_test));
	return group;
}
