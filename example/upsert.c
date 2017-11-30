
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
#include <string.h>
#include <assert.h>

#include <sophia.h>

static int
upsert_callback(int count,
                char **src,    uint32_t *src_size,
                char **upsert, uint32_t *upsert_size,
                char **result, uint32_t *result_size,
                void *arg)
{
	(void)count;
	(void)arg;
	(void)src_size;
	(void)upsert_size;
	(void)result_size;
	/* by default all result keys are automatically
	 * initialized the ones in upsert */
	if (src == NULL) {
		/* Handle first upsert as insert */
		return 0;
	}
	/* copy value field from upsert */
	result[1] = malloc(sizeof(uint32_t));
	if (result[1] == NULL)
		return -1;
	/* increment "id" field */
	*((uint32_t*)result[1]) =
		*(uint32_t*)src[1] + *(uint32_t*)upsert[1];
	return 0;
}

int main(int argc, char *argv[])
{
	(void)argc;
	(void)argv;

	/*
	 * Upsert operation is strictly append-only and executed
	 * during compaction or search times.
	 *
	 * Example:
	 *
	 * Use upsert operation to update a key value by
	 * incrementing it. This example (program) can be run
	 * multiple times.
	*/

	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	sp_setstring(env, "db.test.upsert", (char*)(intptr_t)upsert_callback, 0);
	sp_setstring(env, "db", "key", 0);
	sp_setstring(env, "db.test.scheme", "key", 0);
	sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0);
	sp_setstring(env, "db.test.scheme", "id", 0);
	sp_setstring(env, "db.test.scheme.id", "u32", 0);
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	void *db = sp_getobject(env, "db.test");

	/* increment key 10 times */
	uint32_t key = 1234;
	uint32_t increment = 1;
	int i = 0;
	while (i < 10) {
		void *o = sp_document(db);
		sp_setstring(o, "key", &key, sizeof(key));
		sp_setint(o, "id", increment);
		rc = sp_upsert(db, o);
		if (rc == -1)
			goto error;
		i++;
	}

	/* get */
	void *o = sp_document(db);
	sp_setstring(o, "key", &key, sizeof(key));
	o = sp_get(db, o);
	if (o == NULL)
		goto error;
	printf("get result: %d\n", (int)sp_getint(o, "id"));
	sp_destroy(o);

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
