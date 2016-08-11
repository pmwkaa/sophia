
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

#include <assert.h>
#include <time.h>
#include <sys/time.h>

static inline void
print_current(int i) {
	if (i > 0 && (i % 100000) == 0) {
		fprintf(st_r.output, " %.1fM", i / 1000000.0);
		fflush(st_r.output);
	}
}

static void
mt_set_delete_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 5) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	char value[100];
	memset(value, 0, sizeof(value));
	uint32_t n = 700000;
	uint32_t i, k;
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		print_current(i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", value, sizeof(value)) == 0 );
		t( sp_delete(db, o) == 0 );
		print_current(i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(db, o);
		t( o == NULL );
		print_current(i);
	}
	t( sp_destroy(env) == 0 );
}

static void
mt_set_get_kv_multipart(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 5) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "string,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key_b", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key_b", "u32,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t n = 500000;
	uint32_t i;

	char key_a[] = "very_long_long_key_part";
	srand(82351);
	for (i = 0; i < n; i++) {
		uint32_t key_b = rand();
		uint32_t value = key_b;
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", key_a, sizeof(key_a)) == 0 );
		t( sp_setstring(o, "key_b", &key_b, sizeof(key_b)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		print_current(i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		uint32_t key_b = rand();
		uint32_t value = key_b;
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", key_a, sizeof(key_a)) == 0 );
		t( sp_setstring(o, "key_b", &key_b, sizeof(key_b)) == 0 );
		o = sp_get(db, o);
		t( o != NULL );
		int size = 0;
		t( memcmp(sp_getstring(o, "key", &size), key_a, sizeof(key_a)) == 0 );
		t( *(uint32_t*)sp_getstring(o, "key_b", &size) == key_b );
		t( *(uint32_t*)sp_getstring(o, "value", &size) == value );
		sp_destroy(o);
		print_current(i);
	}

	t( sp_destroy(env) == 0 );
}

static void
mt_set_expire(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 5) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.expire", 1) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "ttl", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.ttl", "u32,timestamp,expire", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t n = 700000;
	uint32_t i, k;

	char value[100];
	memset(value, 0, sizeof(value));

	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_set(db, o) == 0 );
		print_current(i);
	}

	int64_t count = sp_getint(env, "db.test.index.count");
	fprintf(st_r.output, " (count: %"PRIu64")", count);

	t( sp_destroy(env) == 0 );
}

stgroup *multithread_be_group(void)
{
	stgroup *group = st_group("mt_backend");
	st_groupadd(group, st_test("set_delete_get", mt_set_delete_get));
	st_groupadd(group, st_test("set_get_kv_multipart", mt_set_get_kv_multipart));
	st_groupadd(group, st_test("set_expire", mt_set_expire));
	return group;
}

static void
mt_setget(void)
{
	char value[100];
	memset(value, 0, sizeof(value));
	uint32_t n = 300000;
	uint32_t i, k;
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		void *o = sp_document(st_r.db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(st_r.db, o) == 0 );
		print_current(i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		void *o = sp_document(st_r.db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(st_r.db, o);
		t( o != NULL );
		t( *(uint32_t*)sp_getstring(o, "value", NULL) == k );
		sp_destroy(o);
		print_current(i);
	}
}

stgroup *multithread_be_multipass_group(void)
{
	stgroup *group = st_group("mt_backend_multipass");
	st_groupadd(group, st_test("setget", mt_setget));
	return group;
}
