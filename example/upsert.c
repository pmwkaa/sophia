
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
upsert_callback(char **result,
                char **key, int *key_size, int key_count,
                char *src, int src_size,
                char *upsert, int upsert_size,
                void *arg)
{
	(void)key;
	(void)key_size;
	(void)key_count;
	(void)arg;

	assert(upsert != NULL);
	char *c = malloc(upsert_size);
	if (c == NULL)
		return -1;
	*result = c;
	if (src == NULL) {
		memcpy(c, upsert, upsert_size);
		return upsert_size;
	}
	assert(src_size == upsert_size);
	memcpy(c, src, src_size);
	*(uint32_t*)c += *(uint32_t*)upsert;
	return upsert_size;
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
	 * incrementing it.
	*/

	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	sp_setstring(env, "db.test.index.upsert", (char*)(intptr_t)upsert_callback, 0);
	void *db = sp_getobject(env, "db.test");
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	/* schedule key increment */
	uint32_t key = 1234;
	uint32_t increment = 1;
	int i = 0;
	while (i < 10) {
		void *o = sp_document(db);
		sp_setstring(o, "key", &key, sizeof(key));
		sp_setstring(o, "value", &increment, sizeof(increment));
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
	char *ptr = sp_getstring(o, "value", NULL);
	printf("get result: %d\n", *(uint32_t*)ptr);
	assert(*(uint32_t*)ptr >= 10);
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
