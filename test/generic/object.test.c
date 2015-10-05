
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
object_db(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *o = sp_object(db);
	t(o != NULL);
	sp_destroy(o);

	sp_destroy(env);
}

static void
object_set_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	int size = 0;
	t( *(int*)sp_getstring(o, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_getstring(o, "value", &size) == key );
	t( size == sizeof(key) );
	sp_destroy(o);

	sp_destroy(env);
}

static void
object_lsn0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_getint(o, "lsn") == -1 );
	t( sp_set(db, o) == 0 );
	void *c = sp_cursor(env);
	o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "order", ">", 0) == 0 );
	o = sp_get(c, o);
	t( o != NULL );
	int size = 0;
	t( *(int*)sp_getstring(o, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_getstring(o, "value", &size) == key );
	t( size == sizeof(key) );
	t( sp_getint(o, "lsn") > 0 );
	o = sp_get(c, o);
	t( o == NULL );

	sp_destroy(c);
	sp_destroy(env);
}

static void
object_readonly0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o!= NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == -1 );
	t( *(int*)sp_getstring(o, "key", NULL) == key );
	sp_destroy(o);

	sp_destroy(env);
}

static void
object_readonly1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	void *c = sp_cursor(env);
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">", 0) == 0 );
	o = sp_get(c, o);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == -1 );
	sp_destroy(o);
	sp_destroy(c);

	sp_destroy(env);
}

static void
object_reuse(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "order", ">", 0) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( sp_delete(db, o) == 0 );

	o = sp_object(db);
	t(o != NULL);
	t( sp_setstring(o, "order", ">", 0) == 0 );
	o = sp_get(db, o);
	t( o == NULL );

	sp_destroy(env);
}

static void
object_immutable(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_setint(o, "immutable", 1) == 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	void *r = sp_get(db, o);
	t( r != NULL );
	sp_destroy(r);

	t( sp_delete(db, o) == 0 );

	r = sp_get(db, o);
	t( r == NULL );

	t( sp_setint(o, "immutable", 0) == 0 );
	t( sp_destroy(o) == 0 );

	sp_destroy(env);
}

stgroup *object_group(void)
{
	stgroup *group = st_group("object");
	st_groupadd(group, st_test("db", object_db));
	st_groupadd(group, st_test("setget", object_set_get));
	st_groupadd(group, st_test("lsn0", object_lsn0));
	st_groupadd(group, st_test("readonly0", object_readonly0));
	st_groupadd(group, st_test("readonly1", object_readonly1));
	st_groupadd(group, st_test("reuse", object_reuse));
	st_groupadd(group, st_test("immutable", object_immutable));
	return group;
}
