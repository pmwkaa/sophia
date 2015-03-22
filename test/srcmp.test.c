
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>

static void
srcmp_u32(stc *cx srunused)
{
	srcomparator cmp;
	t( sr_cmpset(&cmp, "u32") == 0 );
	uint32_t a = 0;
	uint32_t b = 0;
	t( sr_compare(&cmp, (char*)&a, sizeof(a), (char*)&b, sizeof(b)) == 0 );
	a = 0;
	b = 1;
	t( sr_compare(&cmp, (char*)&a, sizeof(a), (char*)&b, sizeof(b)) == -1 );
	a = 1;
	b = 0;
	t( sr_compare(&cmp, (char*)&a, sizeof(a), (char*)&b, sizeof(b)) == 1 );
}

static void
srcmp_string(stc *cx srunused)
{
	srcomparator cmp;
	t( sr_cmpset(&cmp, "string") == 0 );
	t( sr_compare(&cmp, "test", 4, "test", 4) == 0 );
}

static int
srcmp_custom(char *a, size_t asz, char *b, size_t bsz, void *arg)
{
	return sr_cmpu32(a, asz, b, bsz, arg);
}

static void
srcmp_pointer(stc *cx srunused)
{
	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)srcmp_custom);
	srcomparator cmp;
	t( sr_cmpset(&cmp, pointer) == 0 );
	t( sr_cmpsetarg(&cmp, pointer) == 0 );
	t( cmp.cmparg == (void*)(uintptr_t)srcmp_custom );
	uint32_t a = 0;
	uint32_t b = 0;
	t( sr_compare(&cmp, (char*)&a, sizeof(a), (char*)&b, sizeof(b)) == 0 );
	a = 0;
	b = 1;
	t( sr_compare(&cmp, (char*)&a, sizeof(a), (char*)&b, sizeof(b)) == -1 );
	a = 1;
	b = 0;
	t( sr_compare(&cmp, (char*)&a, sizeof(a), (char*)&b, sizeof(b)) == 1 );
}

static void
srcmp_unknown(stc *cx srunused)
{
	srcomparator cmp;
	t( sr_cmpset(&cmp, "unknown") == -1 );
}

static void
srcmp_prefix(stc *cx srunused)
{
	srcomparator cmp;
	t( sr_cmpset(&cmp, "string") == 0 );
	t( sr_cmpset_prefix(&cmp, "string_prefix") == 0 );
	t( sr_compareprefix(&cmp, "test", 4, "te", 2) == 0 );
	t( sr_compareprefix(&cmp, "testt", 5, "test", 4) == 0 );
}

stgroup *srcmp_group(void)
{
	stgroup *group = st_group("srcmp");
	st_groupadd(group, st_test("u32", srcmp_u32));
	st_groupadd(group, st_test("string", srcmp_string));
	st_groupadd(group, st_test("pointer", srcmp_pointer));
	st_groupadd(group, st_test("unknown", srcmp_unknown));
	st_groupadd(group, st_test("prefix", srcmp_prefix));
	return group;
}
