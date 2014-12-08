
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
sdssiter_test0(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	sr r;
	sr_init(&r, NULL, &a, NULL, NULL, NULL);
	sdss s;
	t( sd_ssinit(&s, &r) == 0 );
	sriter i;
	sr_iterinit(&i, &sd_ssiter, &r);
	t( sr_iteropen(&i, &s.buf, 1) == 0 );
	t( sr_iterof(&i) == NULL );
	t( sd_ssfree(&s, &r) == 0 );
}

static void
sdssiter_test1(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	sr r;
	sr_init(&r, NULL, &a, NULL, NULL, NULL);
	sdss s;
	t( sd_ssinit(&s, &r) == 0 );
	t( sd_ssadd(&s, &r, 1, "a") == 0 );
	t( sd_ssadd(&s, &r, 2, "b") == 0 );
	t( sd_ssadd(&s, &r, 2, "c") == 0 );
	sriter i;
	sr_iterinit(&i, &sd_ssiter, &r);
	t( sr_iteropen(&i, &s.buf, 1) == 1 );
	sdssrecord *rp = sr_iterof(&i);
	t( strcmp(rp->name, "a") == 0 );
	sr_iternext(&i);
	rp = sr_iterof(&i);
	t( strcmp(rp->name, "b") == 0 );
	sr_iternext(&i);
	rp = sr_iterof(&i);
	t( strcmp(rp->name, "c") == 0 );
	sr_iternext(&i);
	rp = sr_iterof(&i);
	t( rp == NULL );
	t( sd_ssfree(&s, &r) == 0 );
}

static void
sdssiter_test2(stc *cx srunused)
{
	sra a;
	sr_allocopen(&a, &sr_astd);
	sr r;
	sr_init(&r, NULL, &a, NULL, NULL, NULL);
	sdss s;
	t( sd_ssinit(&s, &r) == 0 );
	t( sd_ssadd(&s, &r, 1, "a") == 0 );
	t( sd_ssadd(&s, &r, 2, "b") == 0 );
	t( sd_ssadd(&s, &r, 2, "c") == 0 );
	t( sd_ssadd(&s, &r, 2, "d") == 0 );
	t( sd_ssdelete(&s, &r, "b") == 0 );
	t( sd_ssdelete(&s, &r, "b") == -1 );
	t( sd_ssdelete(&s, &r, "c") == 0 );
	sriter i;
	sr_iterinit(&i, &sd_ssiter, &r);
	t( sr_iteropen(&i, &s.buf, 1) == 1 );
	sdssrecord *rp = sr_iterof(&i);
	t( strcmp(rp->name, "a") == 0 );
	sr_iternext(&i);
	rp = sr_iterof(&i);
	t( strcmp(rp->name, "d") == 0 );
	sr_iternext(&i);
	rp = sr_iterof(&i);
	t( rp == NULL );
	t( sd_ssfree(&s, &r) == 0 );
}

stgroup *sdssiter_group(void)
{
	stgroup *group = st_group("sdssiter");
	st_groupadd(group, st_test("test0", sdssiter_test0));
	st_groupadd(group, st_test("test1", sdssiter_test1));
	st_groupadd(group, st_test("test2", sdssiter_test2));
	return group;
}
