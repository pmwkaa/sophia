
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
shutdown_destroy(void)
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

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *dbp = sp_getobject(env, "db.test");
	t( dbp == NULL );

	t( sp_setint(env, "scheduler.run", 0) == 1 ); /* proceed shutdown */

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction0(void)
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
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	void *txn = sp_begin(env);
	t( txn != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *dbp = sp_getobject(env, "db.test");
	t( dbp == NULL );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_destroy(txn) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 ); /* proceed shutdown */

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	void *txn = sp_begin(env);
	t( txn != NULL );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *dbp = sp_getobject(env, "db.test");
	t( dbp == NULL );

	/* process shutdown, txn not binded */
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_destroy(txn) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction2(void)
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
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	void *txn = sp_begin(env);
	t( txn != NULL );

	/* shutdown properly closes used index */
	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction3(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	void *a = sp_begin(env);
	t( a != NULL );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(env);
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *v = sp_begin(env);
	t( v != NULL );

	void *dbp = sp_getobject(env, "db.test");
	t( dbp == NULL );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction4(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	void *a = sp_begin(env);
	t( a != NULL );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *o = sp_object(db);
	t( o != NULL );
	uint32_t key = 7;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == -1 );

	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_destroy(a) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction5(void)
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
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(env);
	t( a != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *o = sp_object(db);
	t( o != NULL );
	uint32_t key = 7;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 ); /* ok */

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction6(void)
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
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(env);
	t( a != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *o = sp_object(db);
	t( o != NULL );
	uint32_t key = 7;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 ); /* ok */

	key = 8;
	o = sp_object(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == -1 );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_cursor0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "a", 0) == 0 );
	t( sp_setint(env, "db.a.sync", 0) == 0 );
	void *dba = sp_getobject(env, "db.a");

	void *o = sp_object(dba);
	void *a = sp_cursor(dba, o);
	t( a != NULL );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	o = sp_object(dba);
	void *b = sp_cursor(dba, o);
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	o = sp_object(dba);
	void *v = sp_cursor(dba, o);
	t( v != NULL );

	void *dbp = sp_getobject(env, "db.test");
	t( dbp == NULL );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	sp_destroy(dba); /* shutdown: dba is offline */
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_cursor1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "a", 0) == 0 );
	t( sp_setint(env, "db.a.sync", 0) == 0 );
	void *dba = sp_getobject(env, "db.a");
	t( sp_open(dba) == 0 );

	void *o = sp_object(dba);
	void *a = sp_cursor(dba, o);
	t( a != NULL );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	o = sp_object(dba);
	void *b = sp_cursor(dba, o);
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	o = sp_object(dba);
	void *v = sp_cursor(dba, o);
	t( v != NULL );

	void *dbp = sp_getobject(env, "db.test");
	t( dbp == NULL );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	sp_destroy(dba); /* unref */
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	sp_destroy(dba); /* schedule shutdown */
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_snapshot0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "snapshot", "a", 0) == 0 );
	void *a = sp_getobject(env, "snapshot.a");
	t( a != NULL );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(db) == 0 );

	t( sp_setstring(env, "snapshot", "b", 0) == 0 );
	void *b = sp_getobject(env, "snapshot.b");
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	t( sp_setstring(env, "snapshot", "v", 0) == 0 );
	void *v = sp_getobject(env, "snapshot.v");
	t( v != NULL );

	void *dbp = sp_getobject(env, "db.test");
	t( dbp == NULL );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_destroy(env) == 0 );
}

stgroup *shutdown_group(void)
{
	stgroup *group = st_group("shutdown");
	st_groupadd(group, st_test("destroy", shutdown_destroy));
	st_groupadd(group, st_test("transaction0", shutdown_transaction0));
	st_groupadd(group, st_test("transaction1", shutdown_transaction1));
	st_groupadd(group, st_test("transaction2", shutdown_transaction2));
	st_groupadd(group, st_test("transaction3", shutdown_transaction3));
	st_groupadd(group, st_test("transaction4", shutdown_transaction4));
	st_groupadd(group, st_test("transaction5", shutdown_transaction5));
	st_groupadd(group, st_test("transaction6", shutdown_transaction6));
	st_groupadd(group, st_test("cursor0", shutdown_cursor0));
	st_groupadd(group, st_test("cursor1", shutdown_cursor1));
	st_groupadd(group, st_test("snapshot0", shutdown_snapshot0));
	return group;
}
