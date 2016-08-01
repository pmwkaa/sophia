
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
	 * Do set, get, delete operations in transaction.
	 *
	 * The only difference between crud.c is that
	 * first argument to sp_set(), sp_get() and sp_delete() is
	 * a transaction object (not database).
	*/

	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	void *db = sp_getobject(env, "db.test");
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	/* begin transaction */
	void *tx = sp_begin(env);

	/* set */
	uint32_t key = 1;
	void *o = sp_document(db);
	sp_setstring(o, "key", &key, sizeof(key));
	sp_setstring(o, "value", &key, sizeof(key));
	rc = sp_set(tx, o);
	if (rc == -1)
		goto error;

	/* get */
	o = sp_document(db);
	sp_setstring(o, "key", &key, sizeof(key));
	o = sp_get(tx, o);
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
	o = sp_document(db);
	sp_setstring(o, "key", &key, sizeof(key));
	rc = sp_delete(tx, o);
	if (rc == -1)
		goto error;

	/* commit transaction */
	rc = sp_commit(tx);
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
