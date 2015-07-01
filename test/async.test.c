
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

static int events = 0;

static void
on_event(void *arg ssunused)
{
	events++;
}

static void
async_set(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "scheduler.on_event", on_event, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *async = sp_asynchronous(db);
	t( async != NULL );

	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(async, o) == 0 );

	t( sp_poll(env) == NULL );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 1 );

	o = sp_poll(env);
	t( o != NULL );
	t( sp_getint(o, "status") == 0 );
	t( strcmp(sp_getstring(o, "type", 0), "set") == 0 );
	t( sp_destroy(o) == 0 );
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "scheduler.on_event", on_event, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *async = sp_asynchronous(db);
	t( async != NULL );

	events = 0;

	uint32_t key = 7;
	void *o = sp_object(async);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(async, o) == 0 );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 1 );

	o = sp_object(async);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(async, o);
	t( o != NULL );

	t( strcmp(sp_getstring(o, "type", 0), "get") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_getobject(o, "result") == NULL );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 2 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "set") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );
	o = sp_poll(env);
	t( o != NULL );

	t( strcmp(sp_getstring(o, "type", 0), "get") == 0 );
	t( sp_getint(o, "status") == 1 );
		void *req = o;
		o = sp_getobject(req, "result");
		t( o != NULL );
		t( *(int*)sp_getstring(o, "value", NULL) == key );
	t( sp_destroy(req) == 0 );
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_transaction(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "scheduler.on_event", on_event, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *async = sp_asynchronous(env);
	t( async != NULL );

	events = 0;
	void *tx = sp_begin(async); // 0
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	tx = sp_begin(async); // 0
	t( tx != NULL );
	key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	t( sp_poll(env) == NULL );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 8 );

	// - - - -
	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "begin") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "set") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "set") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "commit") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	// - - - -
	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "begin") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "set") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "set") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "commit") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	// - - - -
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_cursor(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "scheduler.on_event", on_event, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	events = 0;
	void *tx = sp_begin(env);
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	void *async = sp_asynchronous(db);
	t( async != NULL );

	o = sp_object(async);
	void *cur = sp_cursor(async, o); // 0

	t( sp_poll(env) == NULL );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 1 );

	// - - - -
	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "cursor") == 0 );
	t( sp_getint(o, "status") == 1 );
	t( sp_destroy(o) == 0 );

		t( sp_get(cur, NULL) != NULL ); // 1
		t( sp_setint(env, "scheduler.run", 0) == 0 );
		t( events == 2 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "cursor_get") == 0 );
	t( sp_getint(o, "status") == 1 );
	void *req = o;
	o = sp_getobject(req, "result");
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	t( sp_destroy(req) == 0 );

		t( sp_get(cur, NULL) != NULL ); // 1
		t( sp_setint(env, "scheduler.run", 0) == 0 );
		t( events == 3 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "cursor_get") == 0 );
	t( sp_getint(o, "status") == 1 );
	req = o;
	o = sp_getobject(req, "result");
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	t( sp_destroy(req) == 0 );

		t( sp_get(cur, NULL) != NULL ); // 2
		t( sp_setint(env, "scheduler.run", 0) == 0 );
		t( events == 4 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "cursor_get") == 0 );
	t( sp_getint(o, "status") == 1 );
	req = o;
	o = sp_getobject(req, "result");
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	t( sp_destroy(req) == 0 );

		t( sp_get(cur, NULL) != NULL ); // 3
		t( sp_setint(env, "scheduler.run", 0) == 0 );
		t( events == 5 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "cursor_get") == 0 );
	t( sp_getint(o, "status") == 0 );
	req = o;
	o = sp_getobject(req, "result");
	t( o == NULL );
	t( sp_destroy(req) == 0 );

		t( sp_destroy(cur) == 0 );
		t( sp_setint(env, "scheduler.run", 0) == 0 );
		t( events == 6 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "cursor_destroy") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	// - - - -
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_free0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "scheduler.on_event", on_event, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *async = sp_asynchronous(env);
	t( async != NULL );

	events = 0;
	void *tx = sp_begin(async); // 0
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	tx = sp_begin(async); // 0
	t( tx != NULL );
	key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	t( sp_destroy(env) == 0 );
}

static void
async_free1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "scheduler.on_event", on_event, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *async = sp_asynchronous(env);
	t( async != NULL );

	events = 0;
	void *tx = sp_begin(env);
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	async = sp_asynchronous(db);
	t( async != NULL );

	o = sp_object(async);
	sp_cursor(async, o);
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *async_group(void)
{
	stgroup *group = st_group("async");
	st_groupadd(group, st_test("set", async_set));
	st_groupadd(group, st_test("get", async_get));
	st_groupadd(group, st_test("transaction", async_transaction));
	st_groupadd(group, st_test("cursor", async_cursor));
	st_groupadd(group, st_test("free0", async_free0));
	st_groupadd(group, st_test("free1", async_free1));
	return group;
}
