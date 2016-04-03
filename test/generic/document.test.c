
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
document_db(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *o = sp_document(db);
	t(o != NULL);
	sp_destroy(o);

	sp_destroy(env);
}

static void
document_set_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_document(db);
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
document_lsn0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_document(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_getint(o, "lsn") == -1 );
	t( sp_set(db, o) == 0 );
	void *c = sp_cursor(env);
	o = sp_document(db);
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
document_readonly0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_document(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_document(db);
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
document_readonly1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_document(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	void *c = sp_cursor(env);
	o = sp_document(db);
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
document_reuse(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_document(db);
	t(o != NULL);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_document(db);
	t(o != NULL);
	t( sp_setstring(o, "order", ">", 0) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( sp_delete(db, o) == 0 );

	o = sp_document(db);
	t(o != NULL);
	t( sp_setstring(o, "order", ">", 0) == 0 );
	o = sp_get(db, o);
	t( o == NULL );

	sp_destroy(env);
}

static void
document_immutable(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 7;
	void *o = sp_document(db);
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

stgroup *document_group(void)
{
	stgroup *group = st_group("document");
	st_groupadd(group, st_test("db", document_db));
	st_groupadd(group, st_test("setget", document_set_get));
	st_groupadd(group, st_test("lsn0", document_lsn0));
	st_groupadd(group, st_test("readonly0", document_readonly0));
	st_groupadd(group, st_test("readonly1", document_readonly1));
	st_groupadd(group, st_test("reuse", document_reuse));
	st_groupadd(group, st_test("immutable", document_immutable));
	return group;
}
