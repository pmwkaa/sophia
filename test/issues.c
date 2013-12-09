
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <string.h>
#include <sophia.h>
#include "test.h"

static char *dbrep = "./rep";

static void
gh_5(void) {
	void *env = sp_env();
	t( env != NULL );
	t( sp_ctl(env, SPDIR, SPO_CREAT|SPO_RDWR, dbrep) == 0 );
	void *db = sp_open(env);
	t( db != NULL );

	char k1[] = "key-1";
	char v1[] = "val-1";
	t( sp_set(db, k1, sizeof(k1), v1, sizeof(v1)) == 0);

	char k2[] = "key-10";
	char v2[] = "val-10";
	t( sp_set(db, k2, sizeof(k2), v2, sizeof(v2)) == 0);

	size_t ressize = 0;
	void *res = NULL;
	t( sp_get(db, k1, sizeof(k1), &res, &ressize) == 1 );

	t( ressize == sizeof(v1) );
	t( strcmp(v1, res) == 0 );

	free(res);

	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

static void
gh_37(void)
{
	const int size = 999;
	const int watermark = 3996;
	const int pagesize = 1022;

	char data[size];
	memset(data, 0, sizeof(data));

	void *env = sp_env();
	int rc = sp_ctl(env, SPDIR, SPO_CREAT | SPO_RDWR, dbrep);
	t(rc == 0);
	if (rc == -1)

	rc = sp_ctl(env, SPMERGE, 0);
	t(rc == 0);

	rc = sp_ctl(env, SPMERGEWM, watermark);
	if (rc == -1)
	t(rc == 0);

	sp_ctl(env, SPPAGE, pagesize);
	t(rc == 0);

	void *db = sp_open(env);
	t(db != NULL);

	int byteCount = 0;
	int i;
	for (i = 0; i < 100000; ++i) {
		rc = sp_set(db, &i, sizeof(int), data, size);
		t(rc == 0);
	}

	byteCount += size;
	if (i != 0 && i % watermark == 0) {
		rc = sp_ctl(db, SPMERGEFORCE);
		t(rc == 0);
	}

	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
	t( rmrf(dbrep) == 0 );
}

int
main(int argc, char *argv[])
{
	rmrf(dbrep);

	test(gh_5);
	test(gh_37);
	return 0;
}
