
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
	 * Do fast atomic write of 100 inserts.
	*/

	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	void *db = sp_getobject(env, "db.test");
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	/* create batch object */
	void *batch = sp_batch(db);

	/* insert 100 keys */
	uint32_t key = 0;
	while (key < 100) {
		void *o = sp_object(db);
		sp_setstring(o, "key", &key, sizeof(key));
		rc = sp_set(batch, o);
		if (rc == -1)
			goto error;
		key++;
	}

	/* write batch */
	rc = sp_commit(batch);
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
