
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
	t( strcmp(s, "2.2") == 0 );
	free(s);
	s = sp_getstring(env, "sophia.version_storage", NULL);
	t( s != NULL );
	t( strcmp(s, "2.2") == 0 );
	free(s);
	t( sp_destroy(env) == 0 );
}

static void
conf_error_injection(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_getint(env, "debug.error_injection.sd_build_0") == 0 );
	t( sp_setint(env, "debug.error_injection.sd_build_0", 1) == 0 );
	t( sp_getint(env, "debug.error_injection.sd_build_0") == 1 );
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

	t( sp_setstring(env, "db", "test", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "string,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );

	t( sp_open(env) == 0 );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == -1 );
	t( sp_setint(env, "db.test.compaction.cache", 0) == -1 );

	t( sp_setint(env, "log.enable", 0) == -1 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == -1 );
	t( sp_setint(env, "log.sync", 0) == -1 );
	t( sp_setint(env, "log.rotate_wm", 0) == -1 );
	t( sp_setint(env, "log.rotate_sync", 0) == -1 );

	t( sp_setint(env, "db.test.compaction.page_size", 0) == -1 );
	t( sp_setint(env, "db.test.compaction.node_size", 0) == -1 );
	t( sp_setstring(env, "db.test.path", "path", 0) == -1 );
	t( sp_setstring(env, "db.test.scheme.key", NULL, 0) == -1 );

	void *o = sp_document(db);
	t( o != NULL );

	char key[1025];
	memset(key, 0, sizeof(key));
	t( sp_setstring(o, "key", key, 1024) == 0 );
	t( sp_setstring(o, "key", key, sizeof(key)) == -1 );
	t( sp_setstring(o, "value", key, (1 << 21) + 1 ) == -1 );
	t( sp_setstring(o, "value", key, (1 << 21)) == 0 );
	sp_destroy(o);

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
	t( sp_setstring(o, "value", "", 0) == 0 );
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
conf_cursor(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key_a", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key_b", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "ttl", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key_a", "string,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key_b", "string,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.ttl", "u32,timestamp,expire", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );

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

static void
conf_limits(void)
{
	void *env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.limit.field", 1024) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "field", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.field", "string", 0) == 0 );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	uint64_t key;
	t( o != NULL );
	t( sp_setstring(env, "key", &key, sizeof(key)) == -1 );
	char field[1050];
	t( sp_setstring(env, "field", field, sizeof(field)) == -1 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *conf_group(void)
{
	stgroup *group = st_group("conf");
	st_groupadd(group, st_test("version", conf_version));
	st_groupadd(group, st_test("error_injection", conf_error_injection));
	st_groupadd(group, st_test("validation0", conf_validation0));
	st_groupadd(group, st_test("validation1", conf_validation1));
	st_groupadd(group, st_test("empty_key", conf_empty_key));
	st_groupadd(group, st_test("cursor", conf_cursor));
	st_groupadd(group, st_test("limits", conf_limits));
	return group;
}
