
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <assert.h>

#include <sophia.h>

int main(int argc, char *argv[])
{
	(void)argc;
	(void)argv;

	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	sp_setstring(env, "db.test.index", "key_j", 0);
	sp_setstring(env, "db.test.index", "key_k", 0);
	sp_setstring(env, "db.test.index.key",   "u32", 0);
	sp_setstring(env, "db.test.index.key_j", "u32", 0);
	sp_setstring(env, "db.test.index.key_k", "u32", 0);
	void *db = sp_getobject(env, "db.test");
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	/* set */
	uint32_t i = 0;
	uint32_t j = 0;
	uint32_t k = 0;
	for (i = 0; i <= 2; i++) {
		for (j = 0; j <= 2; j++) {
			for (k = 0; k <= 2; k++) {
				void *o = sp_document(db);
				sp_setstring(o, "key",   &i, sizeof(i));
				sp_setstring(o, "key_j", &j, sizeof(j));
				sp_setstring(o, "key_k", &k, sizeof(k));
				rc = sp_set(db, o);
				if (rc == -1)
					goto error;
			}
		}
	}

	/* get random key */
	i = 1;
	j = 2;
	k = 0;
	void *o = sp_document(db);
	sp_setstring(o, "key",   &i, sizeof(i));
	sp_setstring(o, "key_j", &j, sizeof(j));
	sp_setstring(o, "key_k", &k, sizeof(k));
	o = sp_get(db, o);
	assert(o != NULL);
	sp_destroy(o);

	/* do forward iteration */
	void *cursor = sp_cursor(env);
	o = sp_document(db);
	while ((o = sp_get(cursor, o))) {
		i = *(uint32_t*)sp_getstring(o, "key", NULL);
		j = *(uint32_t*)sp_getstring(o, "key_j", NULL);
		k = *(uint32_t*)sp_getstring(o, "key_k", NULL);
		printf("%"PRIu32".%"PRIu32".%"PRIu32 "\n", i, j, k);
	}
	sp_destroy(cursor);

	/* finish work */
	sp_destroy(env);
	return 0;

error:;
	int size;
	char *error = sp_getstring(env, "sophia.error", &size);
	printf("error: %s\n", error);
	free(error);
	sp_destroy(env);
	return 1;
}
