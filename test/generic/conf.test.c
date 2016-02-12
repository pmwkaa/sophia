
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
conf_version(void)
{
	void *env = sp_env();
	t( env != NULL );
	char *s = sp_getstring(env, "sophia.version", NULL);
	t( s != NULL );
	t( strcmp(s, "2.1.1") == 0 );
	free(s);
	s = sp_getstring(env, "sophia.version_storage", NULL);
	t( s != NULL );
	t( strcmp(s, "2.1.1") == 0 );
	free(s);
	t( sp_destroy(env) == 0 );
}

static void
conf_error_injection(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_getint(env, "debug.error_injection.si_branch_0") == 0 );
	t( sp_setint(env, "debug.error_injection.si_branch_0", 1) == 0 );
	t( sp_getint(env, "debug.error_injection.si_branch_0") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
conf_compaction(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setint(env, "compaction", 58) == 0 );
	t( sp_getint(env, "compaction.50.mode") == 0 );
	char path[64];
	int i = 10;
	while (i < 100) {
		t( sp_setint(env, "compaction", i) == 0 );
		i += 10;
	}
	i = 10;
	while (i < 100) {
		snprintf(path, sizeof(path), "compaction.%d.branch_wm", i);
		t( sp_getint(env, path) >= 0 );
		i += 10;
	}
	t( sp_destroy(env) == 0 );
}

static void
conf_validation0(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == -1 );
	t( sp_setint(env, "memory.limit", 0) == -1 );

	t( sp_setint(env, "log.enable", 0) == -1 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == -1 );
	t( sp_setint(env, "log.sync", 0) == -1 );
	t( sp_setint(env, "log.rotate_wm", 0) == -1 );
	t( sp_setint(env, "log.rotate_sync", 0) == -1 );
	t( sp_setint(env, "log.two_phase_commit", 0) == -1 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );
	t( sp_setint(env, "db.test.page_size", 0) == -1 );
	t( sp_setint(env, "db.test.node_size", 0) == -1 );
	t( sp_setstring(env, "db.test.path", "path", 0) == -1 );
	t( sp_setstring(env, "db.test.index.key", NULL, 0) == -1 );

	void *o = sp_document(db);
	t( o != NULL );

	char key[65000];
	memset(key, 0, sizeof(key));
	t( sp_setstring(o, "key", key, sizeof(key)) == -1 );
	t( sp_setstring(o, "key", key, (1 << 15)) == 0 );
	t( sp_setstring(o, "value", key, (1 << 21) + 1 ) == -1 );
	t( sp_setstring(o, "value", key, (1 << 21)) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
conf_validation1(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "scheduler.threads", NULL, 0) == -1 );
	t( sp_getobject(env, "scheduler.threads") == NULL );

	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "log.enable", NULL, 0) == -1 );
	t( sp_getobject(env, "log.enable") == NULL );

	t( sp_setstring(env, "scheduler.run", NULL, 0) == 0 );
	t( sp_getobject(env, "scheduler.run") == NULL );

	t( sp_destroy(env) == 0 );
}

static int
conf_validation_upsert_op(void *arg, char *src, int src_size,
                          char *upsert, int upsert_size,
                          char **result)
{
	(void)arg;
	(void)src;
	(void)src_size;
	(void)upsert;
	(void)upsert_size;
	(void)result;
	return -1;
}

static void
conf_validation_upsert(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.upsert", conf_validation_upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.format", "document", 0) == 0 );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );
}

static void
conf_threads(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	/* run more threads */
	t( sp_setint(env, "scheduler.threads", 2) == 0 );
	t( sp_setint(env, "scheduler.threads", 3) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
conf_empty_key(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "log.rotate_wm", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	t( sp_setstring(o, "key", "", 0) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_document(db);
	t( sp_setstring(o, "key", "", 0) == 0 );
	o = sp_get(db, o);
	t( o != NULL );

	int key_size;
	void *key = sp_getstring(o, "key", &key_size);
	t( key_size == 0 );
	t( key != NULL );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
conf_db(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key_b", 0) == 0 );
	t( sp_setint(env, "db.test.id", 777) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	char *s = sp_getstring(db, "name",  NULL);
	t( strcmp(s, "test") == 0 );
	free(s);
	t( sp_getint(db, "id") == 777 );
	t( sp_getint(db, "key-count") == 2 );
	s = sp_getstring(env, "db.test.index.key", 0);
	t( s != NULL );
	t( strcmp(s, "string") == 0 );
	free(s);
	s = sp_getstring(env, "db.test.index.key_b", 0);
	t( s != NULL );
	t( strcmp(s, "string") == 0 );
	free(s);
	t( sp_destroy(env) == 0 );
}

static void
conf_cursor(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key_b", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "view", "test_view0", 0) == 0 );

	fprintf(st_r.output, "\n");

	void *cur = sp_getobject(env, NULL);
	t( cur != NULL );
	fprintf(st_r.output, "\n");
	void *o = NULL;
	while ((o = sp_get(cur, o))) {
		char *key = sp_getstring(o, "key", 0);
		char *value = sp_getstring(o, "value", 0);
		fprintf(st_r.output, "%s", key);
		if (value)
			fprintf(st_r.output, " = %s\n", value);
		else
			fprintf(st_r.output, " = \n");
	}
	fprintf(st_r.output, "\n");

	t( sp_destroy(cur) == 0 );
	t( sp_destroy(env) == 0 );
}

stgroup *conf_group(void)
{
	stgroup *group = st_group("conf");
	st_groupadd(group, st_test("version", conf_version));
	st_groupadd(group, st_test("error_injection", conf_error_injection));
	st_groupadd(group, st_test("compaction", conf_compaction));
	st_groupadd(group, st_test("validation0", conf_validation0));
	st_groupadd(group, st_test("validation1", conf_validation1));
	st_groupadd(group, st_test("validation_upsert", conf_validation_upsert));
	st_groupadd(group, st_test("threads", conf_threads));
	st_groupadd(group, st_test("empty_key", conf_empty_key));
	st_groupadd(group, st_test("db", conf_db));
	st_groupadd(group, st_test("cursor", conf_cursor));
	return group;
}
