
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libst.h>

static void
sdss_init(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	sr r;
	sr_init(&r, NULL, &a, NULL, NULL, NULL);
	sdss s;
	t( sd_ssinit(&s) == 0 );
	t( sd_ssfree(&s, &r) == 0 );
}

static void
sdss_add(stc *cx)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	sr r;
	sr_init(&r, NULL, &a, NULL, NULL, NULL);
	sdss s, n;
	t( sd_ssinit(&s) == 0 );
	t( sd_ssadd(&s, &n, &r, 1, "a") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssadd(&s, &n, &r, 2, "b") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssadd(&s, &n, &r, 2, "c") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssadd(&s, &n, &r, 2, "b") == -1 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssfree(&s, &r) == 0 );
}

static void
sdss_delete(stc *cx)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	sr r;
	sr_init(&r, NULL, &a, NULL, NULL, NULL);
	sdss s, n;
	t( sd_ssinit(&s) == 0 );

	t( sd_ssadd(&s, &n, &r, 1, "a") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssdelete(&s, &n, &r, "a") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssadd(&s, &n, &r, 1, "a") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssadd(&s, &n, &r, 1, "b") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssadd(&s, &n, &r, 1, "c") == 0 );
	t( sd_ssfree(&s, &r) == 0 );
	s = n;
	t( sd_ssfree(&s, &r) == 0 );
}

stgroup *sdss_group(void)
{
	stgroup *group = st_group("sdss");
	st_groupadd(group, st_test("init", sdss_init));
	st_groupadd(group, st_test("add", sdss_add));
	st_groupadd(group, st_test("delete", sdss_delete));
	return group;
}
