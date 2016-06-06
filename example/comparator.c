
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

/*
 * example: set custom comparator.
 *
*/

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include <sophia.h>

static int
comparator(char *a, int a_size,
           char *b, int b_size, void *arg)
{
	(void)arg;
	int size = (a_size < b_size) ? a_size : b_size;
	int rc = memcmp(a, b, size);
	if (rc == 0) {
		if (a_size == b_size)
			return 0;
		return (a_size < b_size) ? -1 : 1;
	}
	return rc > 0 ? 1 : -1;
}

int main(int argc, char *argv[])
{
	(void)argc;
	(void)argv;

	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	sp_setstring(env, "db.test.index.comparator", (char*)(intptr_t)comparator, 0);
	sp_setstring(env, "db.test.index.comparator_arg", NULL, 0);
	sp_setstring(env, "db.test.scheme", "document,key(0)", 0);
	sp_setstring(env, "db.test.scheme.key", "string", 0);

	int rc;
	rc = sp_open(env);
	(void)rc;

	sp_destroy(env);
	return 0;
}
