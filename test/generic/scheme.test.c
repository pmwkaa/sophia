
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
scheme_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.compression_cold", "zstd", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key_b", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key_b", "string,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	char *v = sp_getstring(env, "db.test.compression_cold", 0);
	t( v != NULL );
	t( strcmp(v, "zstd") == 0 );
	free(v);

	v = sp_getstring(env, "db.test.scheme.key", 0);
	t( v != NULL );
	t( strcmp(v, "u32,key(0)") == 0 );
	free(v);

	v = sp_getstring(env, "db.test.scheme.key_b", 0);
	t( v != NULL );
	t( strcmp(v, "string,key(1)") == 0 );
	free(v);

	v = sp_getstring(env, "db.test.scheme.value", 0);
	t( v != NULL );
	t( strcmp(v, "string") == 0 );
	free(v);

	t( sp_destroy(env) == 0 );
}

static void
scheme_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.compression_cold", "none", 0) == 0 );
	t( sp_setint(env, "db.test.compression_copy", 1) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key_b", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key_b", "string,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.compression_cold", "zstd", 0) == 0 );
	t( sp_setint(env, "db.test.compression_copy", 1) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "string,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key_b", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key_b", "u64,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	char *v = sp_getstring(env, "db.test.compression_cold", 0);
	t( v != NULL );
	t( strcmp(v, "none") == 0 );
	free(v);

	t( sp_getint(env, "db.test.compression_copy") == 1 );

	v = sp_getstring(env, "db.test.scheme.key", 0);
	t( v != NULL );
	t( strcmp(v, "u32,key(0)") == 0 );
	free(v);

	v = sp_getstring(env, "db.test.scheme.key_b", 0);
	t( v != NULL );
	t( strcmp(v, "string,key(1)") == 0 );
	free(v);

	t( sp_destroy(env) == 0 );
}

static void
scheme_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u16,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key_b", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key_b", "u8,key(1)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );
}

static int
comparator(char *a, int a_size,
           char *b, int b_size, void *arg)
{
	uint32_t av = *(uint32_t*)a;
	uint32_t bv = *(uint32_t*)b;
	if (av == bv)
		return 0;
	return (av > bv) ? 1 : -1;
}

static void
scheme_comparator(void)
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
	t( sp_setstring(env, "db.test.index.comparator", (char*)(intptr_t)comparator, 0) );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	key = 4;
	void *o = sp_document(db);
	sp_setstring(o, "key", &key, sizeof(key));
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	key = 0;
	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));
	void *c = sp_cursor(env);
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == key );
		key++;
	}
	t(key == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
scheme_timestamp0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "ts", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.ts", "u32,timestamp", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	key = 0;
	void *o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	void *c = sp_cursor(env);
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == key );
		t( *(uint32_t*)sp_getstring(o, "ts", NULL) > 0 );
		key++;
	}

	t( sp_destroy(env) == 0 );
}

static void
scheme_timestamp1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "ts0", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.ts0", "u32,timestamp", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "ts1", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.ts1", "u32,timestamp", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	key = 0;
	void *o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	void *c = sp_cursor(env);
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == key );
		t( *(uint32_t*)sp_getstring(o, "ts0", NULL) > 0 );
		t( *(uint32_t*)sp_getstring(o, "ts1", NULL) > 0 );
		key++;
	}

	t( sp_destroy(env) == 0 );
}

static void
scheme_timestamp2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "ts0", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.ts0", "u32,timestamp,expire", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "ts1", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.ts1", "u32,timestamp,expire", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );
}

stgroup *scheme_group(void)
{
	stgroup *group = st_group("scheme");
	st_groupadd(group, st_test("test0", scheme_test0));
	st_groupadd(group, st_test("test1", scheme_test1));
	st_groupadd(group, st_test("test2", scheme_test2));
	st_groupadd(group, st_test("comparator", scheme_comparator));
	st_groupadd(group, st_test("timestamp0", scheme_timestamp0));
	st_groupadd(group, st_test("timestamp1", scheme_timestamp1));
	st_groupadd(group, st_test("timestamp2", scheme_timestamp2));
	return group;
}
