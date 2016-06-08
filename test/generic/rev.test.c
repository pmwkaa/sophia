
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
rev_u8_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u8_rev,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint8_t key = 0;
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

	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	void *c = sp_cursor(env);
	key = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint8_t*)sp_getstring(o, "key", NULL) == key );
		key--;
	}
	t(key == UINT8_MAX); /* overflow */
	sp_destroy(c);

	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	c = sp_cursor(env);
	key = 0;
	while ((o = sp_get(c, o))) {
		t( *(uint8_t*)sp_getstring(o, "key", NULL) == key );
		key++;
	}
	t(key == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
rev_u8_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u8_rev,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint8_t key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	key = 15;
	void *o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	void *c = sp_cursor(env);
	uint8_t expect = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint8_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT8_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint8_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT8_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	sp_setstring(o, "key", &key, sizeof(key));
	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint8_t*)sp_getstring(o, "key", NULL) == expect );
		expect++;
	}
	t(expect == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
rev_u16_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u16_rev,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint16_t key = 0;
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

	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	void *c = sp_cursor(env);
	key = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint16_t*)sp_getstring(o, "key", NULL) == key );
		key--;
	}
	t(key == UINT16_MAX); /* overflow */
	sp_destroy(c);

	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	c = sp_cursor(env);
	key = 0;
	while ((o = sp_get(c, o))) {
		t( *(uint16_t*)sp_getstring(o, "key", NULL) == key );
		key++;
	}
	t(key == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
rev_u16_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u16_rev,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint16_t key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	key = 15;
	void *o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	void *c = sp_cursor(env);
	uint16_t expect = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint16_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT16_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint16_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT16_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	sp_setstring(o, "key", &key, sizeof(key));
	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint16_t*)sp_getstring(o, "key", NULL) == expect );
		expect++;
	}
	t(expect == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
rev_u32_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32_rev,key(0)", 0) == 0 );
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

	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	void *c = sp_cursor(env);
	key = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == key );
		key--;
	}
	t(key == UINT32_MAX); /* overflow */
	sp_destroy(c);

	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	c = sp_cursor(env);
	key = 0;
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == key );
		key++;
	}
	t(key == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
rev_u32_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32_rev,key(0)", 0) == 0 );
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

	key = 15;
	void *o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	void *c = sp_cursor(env);
	uint32_t expect = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT32_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT32_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	sp_setstring(o, "key", &key, sizeof(key));
	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint32_t*)sp_getstring(o, "key", NULL) == expect );
		expect++;
	}
	t(expect == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
rev_u64_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u64_rev,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint64_t key = 0;
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

	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	void *c = sp_cursor(env);
	key = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint64_t*)sp_getstring(o, "key", NULL) == key );
		key--;
	}
	t(key == UINT64_MAX); /* overflow */
	sp_destroy(c);

	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	c = sp_cursor(env);
	key = 0;
	while ((o = sp_get(c, o))) {
		t( *(uint64_t*)sp_getstring(o, "key", NULL) == key );
		key++;
	}
	t(key == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
rev_u64_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u64_rev,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint64_t key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	key = 15;
	void *o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	void *c = sp_cursor(env);
	uint64_t expect = 9;
	while ((o = sp_get(c, o))) {
		t( *(uint64_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT64_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", ">=", 0);
	sp_setstring(o, "key", &key, sizeof(key));

	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint64_t*)sp_getstring(o, "key", NULL) == expect );
		expect--;
	}
	t(expect == UINT64_MAX); /* overflow */
	sp_destroy(c);

	key = 5;
	o = sp_document(db);
	sp_setstring(o, "order", "<=", 0);
	sp_setstring(o, "key", &key, sizeof(key));
	c = sp_cursor(env);
	expect = key;
	while ((o = sp_get(c, o))) {
		t( *(uint64_t*)sp_getstring(o, "key", NULL) == expect );
		expect++;
	}
	t(expect == 10);
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

stgroup *rev_group(void)
{
	stgroup *group = st_group("rev");
	st_groupadd(group, st_test("u8_test0",  rev_u8_test0));
	st_groupadd(group, st_test("u8_test1",  rev_u8_test1));
	st_groupadd(group, st_test("u16_test0", rev_u16_test0));
	st_groupadd(group, st_test("u16_test1", rev_u16_test1));
	st_groupadd(group, st_test("u32_test0", rev_u32_test0));
	st_groupadd(group, st_test("u32_test1", rev_u32_test1));
	st_groupadd(group, st_test("u64_test0", rev_u64_test0));
	st_groupadd(group, st_test("u64_test1", rev_u64_test1));
	return group;
}
