
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
shutdown_destroy(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	t( sp_set(c, "scheduler.run") == 1 ); /* proceed shutdown */

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *txn = sp_begin(env);
	t( txn != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_destroy(txn) == 0 );
	t( sp_set(c, "scheduler.run") == 1 ); /* proceed shutdown */

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );

	void *txn = sp_begin(env);
	t( txn != NULL );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	/* process shutdown, txn not binded */
	t( sp_set(c, "scheduler.run") == 1 );
	t( sp_destroy(txn) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction2(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *txn = sp_begin(env);
	t( txn != NULL );

	/* shutdown properly closes used index */
	t( sp_destroy(env) == 0 );
}

static void
shutdown_transaction3(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );

	void *a = sp_begin(env);
	t( a != NULL );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *b = sp_begin(env);
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *v = sp_begin(env);
	t( v != NULL );

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_set(c, "scheduler.run") == 1 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_cursor0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "db", "a") == 0 );
	void *dba = sp_get(c, "db.a");

	void *o = sp_object(dba);
	void *a = sp_cursor(dba, o);
	t( a != NULL );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	o = sp_object(dba);
	void *b = sp_cursor(dba, o);
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	o = sp_object(dba);
	void *v = sp_cursor(dba, o);
	t( v != NULL );

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_set(c, "scheduler.run") == 1 );

	sp_destroy(dba); /* shutdown: dba is offline */
	t( sp_set(c, "scheduler.run") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_cursor1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "db", "a") == 0 );
	void *dba = sp_get(c, "db.a");
	t( sp_open(dba) == 0 );

	void *o = sp_object(dba);
	void *a = sp_cursor(dba, o);
	t( a != NULL );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	o = sp_object(dba);
	void *b = sp_cursor(dba, o);
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	o = sp_object(dba);
	void *v = sp_cursor(dba, o);
	t( v != NULL );

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_set(c, "scheduler.run") == 1 );

	sp_destroy(dba); /* unref */
	t( sp_set(c, "scheduler.run") == 0 );
	sp_destroy(dba); /* schedule shutdown */
	t( sp_set(c, "scheduler.run") == 1 );

	t( sp_destroy(env) == 0 );
}

static void
shutdown_snapshot0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "snapshot", "a") == 0 );
	void *a = sp_get(c, "snapshot.a");
	t( a != NULL );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	t( sp_set(c, "snapshot", "b") == 0 );
	void *b = sp_get(c, "snapshot.b");
	t( b != NULL );

	sp_destroy(db); /* unref */
	sp_destroy(db); /* schedule shutdown, unlink */

	t( sp_set(c, "snapshot", "v") == 0 );
	void *v = sp_get(c, "snapshot.v");
	t( v != NULL );

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_destroy(a) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(v) == 0 );
	t( sp_set(c, "scheduler.run") == 0 ); /* no unlink */
	t( sp_destroy(b) == 0 );
	t( sp_set(c, "scheduler.run") == 1 );

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
	st_groupadd(group, st_test("cursor0", shutdown_cursor0));
	st_groupadd(group, st_test("cursor1", shutdown_cursor1));
	st_groupadd(group, st_test("snapshot0", shutdown_snapshot0));
	return group;
}
