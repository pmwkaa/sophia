
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libst.h>
#include <sophia.h>

static void
srleb128_test(stc *cx)
{
	unsigned char buffer[16];
	int len;
	uint64_t value;

	len = sr_leb128write(buffer, INT8_MAX);
	t( len == 1 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 1 );
	t( value == INT8_MAX );

	len = sr_leb128write(buffer, UINT8_MAX);
	t( len == 2 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 2 );
	t( value == UINT8_MAX );

	len = sr_leb128write(buffer, INT16_MAX);
	t( len == 3 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 3 );
	t( value == INT16_MAX );

	len = sr_leb128write(buffer, UINT16_MAX);
	t( len == 3 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 3 );
	t( value == UINT16_MAX );

	len = sr_leb128write(buffer, INT32_MAX);
	t( len == 5 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 5 );
	t( value == INT32_MAX );

	len = sr_leb128write(buffer, UINT32_MAX);
	t( len == 5 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 5 );
	t( value == UINT32_MAX );

	len = sr_leb128write(buffer, INT64_MAX);
	t( len == 9 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 9 );
	t( value == INT64_MAX );

	len = sr_leb128write(buffer, UINT64_MAX);
	t( len == 10 );
	value = 0;
	len = sr_leb128read(buffer, &value);
	t( len == 10 );
	t( value == UINT64_MAX );
}

stgroup *srleb128_group(void)
{
	stgroup *group = st_group("leb128");
	st_groupadd(group, st_test("test", srleb128_test));
	return group;
}
