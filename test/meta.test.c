
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
meta_version(void)
{
	void *env = sp_env();
	t( env != NULL );
	char *s = sp_getstring(env, "sophia.version", NULL);
	t( s != NULL );
	t( strcmp(s, "1.2.3") == 0 );
	free(s);
	t( sp_destroy(env) == 0 );
}

static void
meta_error_injection(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_getint(env, "debug.error_injection.si_branch_0") == 0 );
	t( sp_setint(env, "debug.error_injection.si_branch_0", 1) == 0 );
	t( sp_getint(env, "debug.error_injection.si_branch_0") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
meta_scheduler(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setint(env, "scheduler.threads", 2) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_getstring(env, "scheduler.0.trace", NULL) == NULL );
	t( sp_open(env) == 0 );

	char *v = sp_getstring(env, "scheduler.0.trace", NULL);
	t( v != NULL );
	t( strcmp(v, "malfunction") != 0 );
	free(v);

	v = sp_getstring(env, "scheduler.1.trace", NULL);
	t( v != NULL );
	t( strcmp(v, "malfunction") != 0 );
	free(v);

	v = sp_getstring(env, "scheduler.2.trace", NULL);
	t( v == NULL );

	t( sp_destroy(env) == 0 );
}

static void
meta_compaction(void)
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
meta_validation(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == -1 );
	t( sp_setint(env, "memory.limit", 0) == -1 );
	t( sp_setint(env, "compaction.page_size", 0) == -1 );
	t( sp_setint(env, "compaction.node_size", 0) == -1 );
	t( sp_setint(env, "scheduler.threads", 0) == -1 );

	t( sp_setint(env, "log.enable", 0) == -1 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == -1 );
	t( sp_setint(env, "log.sync", 0) == -1 );
	t( sp_setint(env, "log.rotate_wm", 0) == -1 );
	t( sp_setint(env, "log.rotate_sync", 0) == -1 );
	t( sp_setint(env, "log.two_phase_commit", 0) == -1 );
	t( sp_setint(env, "log.commit_lsn", 0) == -1 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );
	t( sp_setstring(env, "db.test.path", "path", 0) == -1 );
	t( sp_setstring(env, "db.test.index.key", NULL, 0) == -1 );

	void *o = sp_object(db);
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
meta_db(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.id", 777) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	char *s = sp_getstring(db, "name",  NULL);
	t( strcmp(s, "test") == 0 );
	free(s);
	t( sp_getint(db, "id") == 777 );
	t( sp_destroy(env) == 0 );
}

static void
meta_cursor(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key_b", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "snapshot", "test_snapshot0", 0) == 0 );

	printf("\n");

	void *o;
	void *cur = sp_cursor(env, NULL);
	t( cur != NULL );
	printf("\n");
	while ((o = sp_get(cur, NULL))) {
		char *key = sp_getstring(o, "key", 0);
		char *value = sp_getstring(o, "value", 0);
		printf("%s", key);
		if (value)
			printf(" = %s\n", value);
		else
			printf(" = \n");
		sp_destroy(o);
	}
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(env) == 0 );
}

stgroup *meta_group(void)
{
	stgroup *group = st_group("meta");
	st_groupadd(group, st_test("version", meta_version));
	st_groupadd(group, st_test("error_injection", meta_error_injection));
	st_groupadd(group, st_test("scheduler", meta_scheduler));
	st_groupadd(group, st_test("compaction", meta_compaction));
	st_groupadd(group, st_test("validation", meta_validation));
	st_groupadd(group, st_test("db", meta_db));
	st_groupadd(group, st_test("cursor", meta_cursor));
	return group;
}
