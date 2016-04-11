
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
#include <libso.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libsc.h>
#include <libse.h>
#include <libst.h>

static inline void
print_current(int i) {
	if (i > 0 && (i % 100000) == 0) {
		fprintf(st_r.output, " %.1fM", i / 1000000.0);
		fflush(st_r.output);
	}
}

static int
upsert_op(int count,
          char **src,    uint32_t *src_size,
          char **upsert, uint32_t *upsert_size,
          char **result, uint32_t *result_size,
          void *arg)
{
	(void)arg;
	if (src == NULL) {
		/* result fields are set to upsert */
		return 0;
	}
	uint32_t v = *(uint32_t*)src[1];
	v += *(uint32_t*)upsert[1];
	result[1] = malloc(sizeof(uint32_t));
	memcpy(result[1], &v, sizeof(v));
	return 0;

#if 0
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
#endif
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
	t( sp_setstring(env, "db.test.upsert", upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
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
	t( sp_setstring(env, "db.test.upsert", upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
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
	t( sp_setstring(env, "db.test.upsert", upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	uint32_t n = 400000;
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

static void
mt_upsert3(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 3) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.upsert", upsert_op, 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	uint32_t n = 200000;
	uint32_t i;
	uint32_t k = 1;
	uint32_t k2 = 2;
	uint32_t value = 1;

	uint32_t prev_0 = 0;
	uint32_t prev_1 = 0;

	void *c0 = sp_cursor(env);
	t( c0 != NULL );

	void *c1 = sp_cursor(env);
	t( c1 != NULL );

	for (i = 0; i < n; i++)
	{
		if (i > 0 && (i % 3000) == 0) {
			void *o = sp_document(db);
			t( o != NULL );
			o = sp_get(c0, o);
			if (o) {
				t( *(uint32_t*)sp_getstring(o, "value", NULL) == prev_0 );

				o = sp_get(c0, o);
				if (o) {
					t( *(uint32_t*)sp_getstring(o, "value", NULL) == prev_0 );
					sp_destroy(o);
				} else {
					t( prev_0 == 0 );
				}

			} else {
				t( prev_0 == 0 );
			}
			sp_destroy(c0);
			prev_0 = i;
			c0 = sp_cursor(env);
			t( c0 != NULL );

		}

		if (i > 0 && (i % 1000) == 0) {
			void *o = sp_document(db);
			t( o != NULL );
			o = sp_get(c1, o);
			if (o) {
				t( *(uint32_t*)sp_getstring(o, "value", NULL) == prev_1 );

				o = sp_get(c1, o);
				if (o) {
					t( *(uint32_t*)sp_getstring(o, "value", NULL) == prev_1 );
					sp_destroy(o);
				} else {
					t( prev_1 == 0 );
				}

			} else {
				t( prev_1 == 0 );
			}
			sp_destroy(c1);
			prev_1 = i;
			c1 = sp_cursor(env);
			t( c1 != NULL );
		}

		if (i > 0 && (i % 5000) == 0) {
			void *o = sp_document(db);
			t( o != NULL );
			t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
			o = sp_get(db, o);
			t( o != NULL );
			t( *(uint32_t*)sp_getstring(o, "value", NULL) == i);
			sp_destroy(o);

			o = sp_document(db);
			t( o != NULL );
			t( sp_setstring(o, "key", &k2, sizeof(k2)) == 0 );
			o = sp_get(db, o);
			t( o != NULL );
			t( *(uint32_t*)sp_getstring(o, "value", NULL) == i);
			sp_destroy(o);
		}

		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_upsert(db, o) == 0 );
		o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k2, sizeof(k2)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_upsert(db, o) == 0 );

		print_current(i);
	}

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c1) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *multithread_upsert_group(void)
{
	stgroup *group = st_group("mt_upsert");
	st_groupadd(group, st_test("upsert0", mt_upsert0));
	st_groupadd(group, st_test("upsert1", mt_upsert1));
	st_groupadd(group, st_test("upsert2", mt_upsert2));
	st_groupadd(group, st_test("upsert3", mt_upsert3));
	return group;
}
