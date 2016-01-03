
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libst.h>

static void
ss_qf_test0(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssqf f;
	ss_qfinit(&f);

	t( ss_qfensure(&f, &a, 100) == 0 );

	int i = 0;
	while (i < 100) {
		uint64_t hash = ss_fnv((char*)&i, sizeof(i));
		ss_qfadd(&f, hash);
		i++;
	}

	int has = 0;
	i = 0;
	while (i < 100) {
		uint64_t hash = ss_fnv((char*)&i, sizeof(i));
		has += ss_qfhas(&f, hash);
		i++;
	}

	t(has == 100);

	while (i < 200) {
		uint64_t hash = ss_fnv((char*)&i, sizeof(i));
		has += ss_qfhas(&f, hash);
		i++;
	}

	t(has == 100);

	fflush(NULL);

	ss_qffree(&f, &a);
}

static void
ss_qf_test1(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssqf f;
	ss_qfinit(&f);

	t( ss_qfensure(&f, &a, 100) == 0 );

	int i = 0;
	while (i < 100) {
		char key[100];
		int keylen = snprintf(key, sizeof(key), "%d", i);
		uint64_t hash = ss_fnv(key, keylen);
		ss_qfadd(&f, hash);
		i++;
	}

	int has = 0;
	i = 0;
	while (i < 100) {
		char key[100];
		int keylen = snprintf(key, sizeof(key), "%d", i);
		uint64_t hash = ss_fnv(key, keylen);
		has += ss_qfhas(&f, hash);
		i++;
	}
	t(has == 100);

	while (i < 200) {
		char key[100];
		int keylen = snprintf(key, sizeof(key), "%d", i);
		uint64_t hash = ss_fnv(key, keylen);
		has += ss_qfhas(&f, hash);
		i++;
	}
	t(has == 162);

	ss_qffree(&f, &a);
}

static void
ss_qf_test2(void)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	ssqf f;
	ss_qfinit(&f);

	t( ss_qfensure(&f, &a, 100) == 0 );
	t( ss_qfensure(&f, &a, 300) == 0 );
	t( ss_qfensure(&f, &a, 2000) == 0 );

	int i = 0;
	while (i < 2000) {
		char key[100];
		int keylen = snprintf(key, sizeof(key), "%d", i);
		uint64_t hash = ss_fnv(key, keylen);
		ss_qfadd(&f, hash);
		i++;
	}

	int has = 0;
	i = 0;
	while (i < 3000) {
		char key[100];
		int keylen = snprintf(key, sizeof(key), "%d", i);
		uint64_t hash = ss_fnv(key, keylen);
		has += ss_qfhas(&f, hash);
		i++;
	}
	t( has >= 2000 );
	t( has == 2246 );

	ss_qffree(&f, &a);
}

stgroup *ss_qf_group(void)
{
	stgroup *group = st_group("ssqf");
	st_groupadd(group, st_test("test0", ss_qf_test0));
	st_groupadd(group, st_test("test1", ss_qf_test1));
	st_groupadd(group, st_test("test2", ss_qf_test2));
	return group;
}
