
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
deadlock_test0(void)
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

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );

	int key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );

	t( sp_getint(tx, "deadlock") == 0 );

	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
deadlock_test1(void)
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

	int rc;
	void *t0 = sp_begin(env);
	t( t0 != NULL );
	void *t1 = sp_begin(env);
	t( t1 != NULL );

	int key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 7;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	t( sp_getint(t0, "deadlock") == 0 );
	t( sp_getint(t1, "deadlock") == 0 );

	rc = sp_commit(t0);
	t( rc == 0 );
	rc = sp_commit(t1);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
deadlock_test2(void)
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

	int rc;
	void *t0 = sp_begin(env);
	t( t0 != NULL );
	void *t1 = sp_begin(env);
	t( t1 != NULL );

	int key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 7;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	rc = sp_commit(t0);
	t( rc == 2 ); /* wait */
	rc = sp_commit(t1);
	t( rc == 2 ); /* wait */

	t( sp_getint(t0, "deadlock") == 1 );
	t( sp_getint(t1, "deadlock") == 1 );

	t( sp_destroy(t0) == 0 ) ;
	t( sp_getint(t1, "deadlock") == 0 );
	rc = sp_commit(t1);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
deadlock_test3(void)
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

	int rc;
	void *t0 = sp_begin(env);
	t( t0 != NULL );
	void *t1 = sp_begin(env);
	t( t1 != NULL );

	int key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 7;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	rc = sp_commit(t0);
	t( rc == 2 ); /* lock */
	rc = sp_commit(t1);
	t( rc == 2 ); /* lock */

	t( sp_getint(t0, "deadlock") == 1 );
	t( sp_getint(t1, "deadlock") == 1 );

	t( sp_destroy(t1) == 0 ) ;
	t( sp_getint(t0, "deadlock") == 0 );
	rc = sp_commit(t0);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *deadlock_group(void)
{
	stgroup *group = st_group("deadlock");
	st_groupadd(group, st_test("test0", deadlock_test0));
	st_groupadd(group, st_test("test1", deadlock_test1));
	st_groupadd(group, st_test("test2", deadlock_test2));
	st_groupadd(group, st_test("test3", deadlock_test3));
	return group;
}
