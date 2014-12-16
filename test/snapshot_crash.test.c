
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
snapshot_case0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "debug.error_injection.se_snapshot_0", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_case1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "debug.error_injection.se_snapshot_1", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_case2(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "debug.error_injection.se_snapshot_2", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_case3(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "debug.error_injection.se_snapshot_3", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_update_case0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "snapshot", "test0") == 0 );

	t( sp_set(c, "debug.error_injection.se_snapshot_0", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test0");
	t( snapshot != NULL );
	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_update_case1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "snapshot", "test0") == 0 );

	t( sp_set(c, "debug.error_injection.se_snapshot_1", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test0");
	t( snapshot != NULL );
	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_update_case2(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "snapshot", "test0") == 0 );

	t( sp_set(c, "debug.error_injection.se_snapshot_2", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test0");
	t( snapshot != NULL );
	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_update_case3(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "snapshot", "test0") == 0 );

	t( sp_set(c, "debug.error_injection.se_snapshot_3", "1") == 0 );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	snapshot = sp_get(c, "snapshot.test0");
	t( snapshot != NULL );
	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );

	t( sp_destroy(env) == 0 );
}

stgroup *snapshot_crash_group(void)
{
	stgroup *group = st_group("snapshot_crash");
	st_groupadd(group, st_test("case0", snapshot_case0));
	st_groupadd(group, st_test("case1", snapshot_case1));
	st_groupadd(group, st_test("case2", snapshot_case2));
	st_groupadd(group, st_test("case3", snapshot_case3));
	st_groupadd(group, st_test("update_case0", snapshot_update_case0));
	st_groupadd(group, st_test("update_case1", snapshot_update_case1));
	st_groupadd(group, st_test("update_case2", snapshot_update_case2));
	st_groupadd(group, st_test("update_case3", snapshot_update_case3));
	return group;
}
