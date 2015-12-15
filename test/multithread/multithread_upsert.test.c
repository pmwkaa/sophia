
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

static int
upsert_op(char **result,
          char **key, int *key_size, int key_count,
          char *src, int src_size,
          char *upsert, int upsert_size,
          void *arg)
{
	(void)key;
	(void)key_size;
	(void)key_count;
	assert(upsert != NULL);
	(void)arg;
	char *c = malloc(upsert_size);
	if (c == NULL)
		return -1;
	*result = c;
	if (src == NULL) {
		memcpy(c, upsert, upsert_size);
		return upsert_size;
	}
	assert(src_size == upsert_size);
	memcpy(c, src, src_size);
	*(uint32_t*)c += *(uint32_t*)upsert;
	return upsert_size;
}

static void
mt_upsert0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 5) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.upsert", upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	uint32_t n = 400000;
	uint32_t i, k = 1234;
	uint32_t value = 1;
	for (i = 0; i < n; i++) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_upsert(db, o) == 0 );
		print_current(i);
	}

	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "value", NULL) == n);
	sp_destroy(o);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
mt_upsert1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 5) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.upsert", upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	uint32_t n = 400000;
	uint32_t i, k = 1234;
	uint32_t value = 1;

	void *tx = sp_begin(env);

	for (i = 0; i < n; i++) {
		if (i > 0 && (i % 1000) == 0) {
			void *o = sp_document(db);
			t( o != NULL );
			t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
			void *c = sp_cursor(env);
			t( c != NULL );
			o = sp_get(c, o);
			t( o != NULL );
			t( *(uint32_t*)sp_getstring(o, "value", NULL) == i);
			sp_destroy(o);
			sp_destroy(c);
		}
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_upsert(db, o) == 0 );

		print_current(i);
	}

	sp_destroy(tx);

	t( sp_destroy(env) == 0 );
}

static void
mt_upsert2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 5) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.upsert", upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	uint32_t n = 700000;
	uint32_t i, k = 1234;
	uint32_t value = 1;

	void *tx = sp_begin(env);
	t( tx != NULL );

	for (i = 0; i < n; i++) {
		if (i > 0 && (i % 1000) == 0) {
			void *o = sp_document(db);
			t( o != NULL );
			t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
			void *c = sp_cursor(env);
			t( c != NULL );
			o = sp_get(c, o);
			t( o != NULL );
			t( *(uint32_t*)sp_getstring(o, "value", NULL) == i);
			sp_destroy(o);
			sp_destroy(c);

			t( sp_commit(tx) == 0 );
			tx = sp_begin(env);
			t( tx != NULL );
		}
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_upsert(db, o) == 0 );

		print_current(i);
	}

	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *multithread_upsert_group(void)
{
	stgroup *group = st_group("mt_upsert");
	st_groupadd(group, st_test("upsert0", mt_upsert0));
	st_groupadd(group, st_test("upsert1", mt_upsert1));
	st_groupadd(group, st_test("upsert2", mt_upsert2));
	return group;
}
