
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

	/*
	 * Do cursor iteration.
	*/

	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", "_test", 0);
	sp_setstring(env, "db", "test", 0);
	void *db = sp_getobject(env, "db.test");
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	/* insert 10 keys */
	uint32_t key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		sp_setstring(o, "key", &key, sizeof(key));
		rc = sp_set(db, o);
		if (rc == -1)
			goto error;
		key++;
	}

	/* create cursor and do forward iteration */
	void *cursor = sp_cursor(env);
	void *o = sp_document(db);
	while ((o = sp_get(cursor, o))) {
		printf("%"PRIu32"\n", *(uint32_t*)sp_getstring(o, "key", NULL));
	}
	sp_destroy(cursor);

	printf("\n");

	/* create cursor and do backward iteration */
	cursor = sp_cursor(env);
	o = sp_document(db);
	sp_setstring(o, "order", "<", 0);
	while ((o = sp_get(cursor, o))) {
		printf("%"PRIu32"\n", *(uint32_t*)sp_getstring(o, "key", NULL));
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
