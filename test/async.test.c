
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libss.h>
#include <libst.h>
#include <sophia.h>

static int events = 0;

static void
on_event(void *arg ssunused)
{
	events++;
}

static void
async_set(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)on_event);
	t( sp_set(c, "scheduler.on_event", pointer, NULL) == 0 );

	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *async = sp_async(db);
	t( async != NULL );

	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(async, o) == 0 );

	t( sp_poll(env) == NULL );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( events == 1 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 1 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( strcmp(sp_get(o, "type"), "set") == 0 );
	t( sp_destroy(o) == 0 );
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_get(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)on_event);
	t( sp_set(c, "scheduler.on_event", pointer, NULL) == 0 );

	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *async = sp_async(db);
	t( async != NULL );

	events = 0;

	uint32_t key = 7;
	void *o = sp_object(async);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(async, o) == 0 );

	t( sp_set(c, "scheduler.run") == 0 );
	t( events == 1 );

	o = sp_object(async);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(async, o);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 2 );
	t( strcmp(sp_get(o, "type"), "get") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_get(o, "result") == NULL );

	t( sp_set(c, "scheduler.run") == 0 );
	t( events == 2 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 1 );
	t( strcmp(sp_get(o, "type"), "set") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 2 );
	t( strcmp(sp_get(o, "type"), "get") == 0 );
	t( *(int*)sp_get(o, "status") == 1 );
		void *req = o;
		o = sp_get(req, "result");
		t( o != NULL );
		t( *(int*)sp_get(o, "value", NULL) == key );
	t( sp_destroy(req) == 0 );
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_transaction(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)on_event);
	t( sp_set(c, "scheduler.on_event", pointer, NULL) == 0 );

	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *async = sp_async(env);
	t( async != NULL );

	events = 0;
	void *tx = sp_begin(async); // 0
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	tx = sp_begin(async); // 0
	t( tx != NULL );
	key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	t( sp_poll(env) == NULL );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_set(c, "scheduler.run") == 0 );
	t( events == 8 );

	// - - - -
	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 1 );
	t( strcmp(sp_get(o, "type"), "begin") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 2 );
	t( strcmp(sp_get(o, "type"), "set") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 3 );
	t( strcmp(sp_get(o, "type"), "set") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 4 );
	t( strcmp(sp_get(o, "type"), "commit") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	// - - - -
	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 5 );
	t( strcmp(sp_get(o, "type"), "begin") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 6 );
	t( strcmp(sp_get(o, "type"), "set") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 7 );
	t( strcmp(sp_get(o, "type"), "set") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 8 );
	t( strcmp(sp_get(o, "type"), "commit") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	// - - - -
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_cursor(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)on_event);
	t( sp_set(c, "scheduler.on_event", pointer, NULL) == 0 );

	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	events = 0;
	void *tx = sp_begin(env);
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	void *async = sp_async(db);
	t( async != NULL );

	o = sp_object(async);
	void *cur = sp_cursor(async, o); // 0

	t( sp_poll(env) == NULL );
	t( sp_set(c, "scheduler.run") == 0 );
	t( events == 1 );

	// - - - -
	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 1 );
	t( strcmp(sp_get(o, "type"), "cursor") == 0 );
	t( *(int*)sp_get(o, "status") == 1 );
	t( sp_destroy(o) == 0 );

		t( sp_get(cur) != NULL ); // 1
		t( sp_set(c, "scheduler.run") == 0 );
		t( events == 2 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 2 );
	t( strcmp(sp_get(o, "type"), "cursor_get") == 0 );
	t( *(int*)sp_get(o, "status") == 1 );
	void *req = o;
	o = sp_get(req, "result");
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	t( sp_destroy(req) == 0 );

		t( sp_get(cur) != NULL ); // 1
		t( sp_set(c, "scheduler.run") == 0 );
		t( events == 3 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 3 );
	t( strcmp(sp_get(o, "type"), "cursor_get") == 0 );
	t( *(int*)sp_get(o, "status") == 1 );
	req = o;
	o = sp_get(req, "result");
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	t( sp_destroy(req) == 0 );

		t( sp_get(cur) != NULL ); // 2
		t( sp_set(c, "scheduler.run") == 0 );
		t( events == 4 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 4 );
	t( strcmp(sp_get(o, "type"), "cursor_get") == 0 );
	t( *(int*)sp_get(o, "status") == 1 );
	req = o;
	o = sp_get(req, "result");
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	t( sp_destroy(req) == 0 );

		t( sp_get(cur) != NULL ); // 3
		t( sp_set(c, "scheduler.run") == 0 );
		t( events == 5 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 5 );
	t( strcmp(sp_get(o, "type"), "cursor_get") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	req = o;
	o = sp_get(req, "result");
	t( o == NULL );
	t( sp_destroy(req) == 0 );

		t( sp_destroy(cur) == 0 );
		t( sp_set(c, "scheduler.run") == 0 );
		t( events == 6 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_type(o), "request") == 0 );
	t( *(uint64_t*)sp_get(o, "seq") == 6 );
	t( strcmp(sp_get(o, "type"), "cursor_destroy") == 0 );
	t( *(int*)sp_get(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	// - - - -
	t( sp_poll(env) == NULL );

	t( sp_destroy(env) == 0 );
}

static void
async_free0(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)on_event);
	t( sp_set(c, "scheduler.on_event", pointer, NULL) == 0 );

	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *async = sp_async(env);
	t( async != NULL );

	events = 0;
	void *tx = sp_begin(async); // 0
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	tx = sp_begin(async); // 0
	t( tx != NULL );
	key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 1
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 ); // 2
	t( sp_commit(tx) == 0 ); // 3

	t( sp_destroy(env) == 0 );
}

static void
async_free1(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)on_event);
	t( sp_set(c, "scheduler.on_event", pointer, NULL) == 0 );

	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *async = sp_async(env);
	t( async != NULL );

	events = 0;
	void *tx = sp_begin(env);
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	async = sp_async(db);
	t( async != NULL );

	o = sp_object(async);
	sp_cursor(async, o);
	t( sp_set(c, "scheduler.run") == 0 );

	sp_get(async, o);
	sp_get(async, o);

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
