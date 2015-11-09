
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

	events = 0;

	uint32_t key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_document(db);
	t( sp_setint(o, "async", 1) == 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "arg", "arg_test", 0) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "on_read") == 0 );
	t( sp_getint(o, "status") == 0 );
	t( sp_destroy(o) == 0 );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 1 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "on_read") == 0 );
	t( sp_getint(o, "status") == 1 );
	t( strcmp(sp_getstring(o, "arg", NULL), "arg_test") == 0 );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	t( sp_destroy(o) == 0 );

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
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	void *cur = sp_cursor(env);

	o = sp_document(db);
	sp_setint(o, "async", 1);
	o = sp_get(cur, o); // 1
	t( o != NULL );
	sp_destroy(o);
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 1 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "on_read") == 0 );
	t( sp_getint(o, "status") == 1 );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	o = sp_get(cur, o); // 2
	sp_destroy(o);

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 2 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "on_read") == 0 );
	t( sp_getint(o, "status") == 1 );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	o = sp_get(cur, o); // 3
	t( o != NULL );
	sp_destroy(o);

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 3 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "on_read") == 0 );
	t( sp_getint(o, "status") == 1 );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	o = sp_get(cur, o); // 4
	t( o != NULL );
	sp_destroy(o);

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( events == 4 );

	o = sp_poll(env);
	t( o != NULL );
	t( strcmp(sp_getstring(o, "type", 0), "on_read") == 0 );
	t( sp_getint(o, "status") == 0 );
	sp_destroy(o);

	t( sp_destroy(cur) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );

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

	events = 0;
	void *tx = sp_begin(env);
	t( tx != NULL );
	uint32_t key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_document(db);
	sp_setint(o, "async", 1);
	sp_get(c, o);
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *async_group(void)
{
	stgroup *group = st_group("async");
	st_groupadd(group, st_test("get", async_get));
	st_groupadd(group, st_test("cursor", async_cursor));
	st_groupadd(group, st_test("free0", async_free0));
	return group;
}
