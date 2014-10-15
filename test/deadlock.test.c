
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>
#include <sophia.h>

static void
deadlock_test0(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.logdir_sync", "0") == 0 );
	t( sp_set(c, "db.test.logdir_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );

	t( sp_set(c, "db.test.run_lockdetect", tx) == 0 );

	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
deadlock_test1(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.logdir_sync", "0") == 0 );
	t( sp_set(c, "db.test.logdir_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int rc;
	void *t0 = sp_begin(db);
	t( t0 != NULL );
	void *t1 = sp_begin(db);
	t( t1 != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	t( sp_set(c, "db.test.run_lockdetect", t0) == 0 );
	t( sp_set(c, "db.test.run_lockdetect", t1) == 0 );

	rc = sp_commit(t0);
	t( rc == 0 );
	rc = sp_commit(t1);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
deadlock_test2(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.logdir_sync", "0") == 0 );
	t( sp_set(c, "db.test.logdir_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int rc;
	void *t0 = sp_begin(db);
	t( t0 != NULL );
	void *t1 = sp_begin(db);
	t( t1 != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	rc = sp_commit(t0);
	t( rc == 2 ); /* wait */
	rc = sp_commit(t1);
	t( rc == 2 ); /* wait */

	t( sp_set(c, "db.test.run_lockdetect", t0) == 1 );
	t( sp_set(c, "db.test.run_lockdetect", t1) == 1 );

	t( sp_rollback(t0) == 0 ) ;
	t( sp_set(c, "db.test.run_lockdetect", t1) == 0 );
	rc = sp_commit(t1);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
deadlock_test3(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.logdir_sync", "0") == 0 );
	t( sp_set(c, "db.test.logdir_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int rc;
	void *t0 = sp_begin(db);
	t( t0 != NULL );
	void *t1 = sp_begin(db);
	t( t1 != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t0, o) == 0 );

	key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(t1, o) == 0 );

	rc = sp_commit(t0);
	t( rc == 2 ); /* lock */
	rc = sp_commit(t1);
	t( rc == 2 ); /* lock */

	t( sp_set(c, "db.test.run_lockdetect", t0) == 1 );
	t( sp_set(c, "db.test.run_lockdetect", t1) == 1 );

	t( sp_rollback(t1) == 0 ) ;
	t( sp_set(c, "db.test.run_lockdetect", t0) == 0 );
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
