
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
#include <libsd.h>
#include <libst.h>

static void
github_97(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.cache", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	/* we must pass sizeof(uint32_t) in sp_setstring() */
	uint32_t i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );   /* < */
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 ); /* < */
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *cur = sp_cursor(env);
	t( cur != NULL );

	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 99;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 ); /* <  */

	i = 0;
	while ((o = sp_get(cur, o)))
		i++;
	t( i == 1 );
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
github_104(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "string,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	char key_a[] = "aa";
	char key_b[] = "bb";
	char key_c[] = "cc";
	char key_d[] = "dd";

	void *o = sp_document(db);
	t( sp_setstring(o, "key", key_a, sizeof(key_a)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_document(db);
	t( sp_setstring(o, "key", key_b, sizeof(key_b)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_document(db);
	t( sp_setstring(o, "key", key_c, sizeof(key_c)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_document(db);
	t( sp_setstring(o, "key", key_d, sizeof(key_d)) == 0 );
	t( sp_set(db, o) == 0 );

	void *cur = sp_cursor(env);
	t( cur != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", key_b, sizeof(key_b)) == 0 );
	t( sp_setstring(o, "order", "<=", 0) == 0 );
	int i = 0;
	while ((o = sp_get(cur, o))) {
		printf(" %s", (char*)sp_getstring(o, "key", 0));
		i++;
	}
	fflush(NULL);
	t( i == 2 );
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
github_112(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	char *s = sp_getstring(env, "db.test.compaction.gc", NULL);
	t( s == NULL );
	t( sp_destroy(env) == 0 );
}

static void
github_117(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	int i = 0;
	int max = 30;
	while (i < max) {
		char name[30];
		snprintf(name, sizeof(name), "db.t_%d", i);
		t( sp_setstring(env, "db", name + 3, 0) == 0 );
		void *db = sp_getobject(env, name);
		t( db != NULL );
		i++;
	}
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
github_118(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	unsigned key = 123456;

	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	t( sp_getint(env, "db.test.index.count") == 1 );
	char *sz = sp_getstring(env, "sophia.status", NULL);
	t( strcmp(sz, "online") == 0 );
	free(sz);

	t( sp_destroy(env) == 0 );
}

static void
github_120(void)
{
	/* open or create environment and database */
	void *env = sp_env();
	sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0);
	sp_setstring(env, "db", "test", 0);
	void *db = sp_getobject(env, "db.test");
	int rc = sp_open(env);
	if (rc == -1)
		goto error;

	/* set */
	void *o = sp_document(db);
	sp_setstring(o, "key", "hello", 0);
	sp_setstring(o, "value", "world", 0);
	rc = sp_set(db, o);
	if (rc == -1)
		goto error;

	/* get */
	o = sp_document(db);
	sp_setstring(o, "key", "hello", 0);
	o = sp_get(db, o);
	if (o) {
		/* ensure key and value are correct */
		int size;
		char *ptr = sp_getstring(o, "key", &size);
		t( size == 5 );
		t( strncmp(ptr, "hello", 5) == 0 );

		ptr = sp_getstring(o, "value", &size);
		t( size == 5 );
		t( strncmp(ptr, "world", 5) == 0 );

		sp_destroy(o);
	}

	/* delete */
	o = sp_document(db);
	sp_setstring(o, "key", "hello", 0);
	rc = sp_delete(db, o);
	if (rc == -1)
		goto error;

	/* finish work */
	sp_destroy(env);
	return;

error:;
	int size;
	char *error = sp_getstring(env, "sophia.error", &size);
	printf("error: %s\n", error);
	free(error);
	sp_destroy(env);
}

static void
github_123(void)
{
	rmrf("./abc");
	rmrf("./abc_log");

	void *env = sp_env();
	t( env != NULL );

	char path[] = { '.', '/', 'a', 'b', 'c' };
	char path_log[] = { '.', '/', 'a', 'b', 'c', '_', 'l', 'o', 'g' };
	char name[] = { 't', 'e', 's', 't' };

	t( sp_setstring(env, "sophia.path", path, sizeof(path)) == 0 );
	t( sp_setstring(env, "db", name, sizeof(name)) == 0 );
	t( sp_setstring(env, "log.path", path_log, sizeof(path_log)) == 0 );
	t( sp_open(env) == 0 );

	t( exists("./", "abc") == 1 );
	t( exists("./", "abc_log") == 1 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	t( sp_destroy(env) == 0 );

	rmrf("./abc");
	rmrf("./abc_log");
}

stgroup *github_group(void)
{
	stgroup *group = st_group("github");
	st_groupadd(group, st_test("ticket_97", github_97));
	st_groupadd(group, st_test("ticket_104", github_104));
	st_groupadd(group, st_test("ticket_112", github_112));
	st_groupadd(group, st_test("ticket_117", github_117));
	st_groupadd(group, st_test("ticket_118", github_118));
	st_groupadd(group, st_test("ticket_120", github_120));
	st_groupadd(group, st_test("ticket_123", github_123));
	return group;
}
