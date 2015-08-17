
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
#include <assert.h>

#include <sophia.h>

int main(int argc, char *argv[])
{
	(void)argc;
	(void)argv;

	/*
	 * Do set, get, delete operations. (see transaction.c)
	*/

	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	void *db = sp_getobject(env, "db.test");
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	/* set */
	uint32_t key = 1;
	void *o = sp_object(db);
	sp_setstring(o, "key", &key, sizeof(key));
	sp_setstring(o, "value", &key, sizeof(key));
	rc = sp_set(db, o);
	if (rc == -1)
		goto error;

	/* get */
	o = sp_object(db);
	sp_setstring(o, "key", &key, sizeof(key));
	o = sp_get(db, o);
	if (o) {
		/* ensure key and value are correct */
		int size;
		char *ptr = sp_getstring(o, "key", &size);
		assert(size == sizeof(uint32_t));
		assert(*(uint32_t*)ptr == key);

		ptr = sp_getstring(o, "value", &size);
		assert(size == sizeof(uint32_t));
		assert(*(uint32_t*)ptr == key);

		sp_destroy(o);
	}

	/* delete */
	o = sp_object(db);
	sp_setstring(o, "key", &key, sizeof(key));
	rc = sp_delete(db, o);
	if (rc == -1)
		goto error;

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
