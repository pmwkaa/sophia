
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
	svinit(&v, &sv_localif, &l, NULL);
	t( svflags(&v) == l.flags );
	svflagsadd(&v, SVDUP);
	t( svflags(&v) == (l.flags|SVDUP) );
	t( svlsn(&v) == l.lsn );
	svlsnset(&v, 8);
	t( svlsn(&v) == 8 );
	t( svkey(&v) == l.key );
	t( svkeysize(&v) == l.keysize );
	t( svvalue(&v) == l.value );
	t( svvaluesize(&v) == l.valuesize );
	t( svvalueoffset(&v) == 0 );
}

stgroup *svlocal_group(void)
{
	stgroup *group = st_group("svlocal");
	st_groupadd(group, st_test("test", svlocal_test));
	return group;
}
