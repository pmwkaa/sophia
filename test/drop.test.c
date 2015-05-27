
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

static void
drop_test(stc *cx ssunused)
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
	t( sp_set(c, "db.test.sync", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *txn = sp_begin(env);
	t( txn != NULL );

	sp_destroy(db); /* unref */
	t( sp_drop(db) == 0); /* scheduler drop */
	sp_destroy(db); /* schedule shutdown, unlink */

	void *dbp = sp_get(c, "db.test");
	t( dbp == NULL );

	t( sp_set(c, "scheduler.run") == 0 );
	t( sp_destroy(txn) == 0 );

	t( exists(cx->suite->dir, "") == 1 );
	t( sp_set(c, "scheduler.run") == 1 ); /* proceed drop */
	t( exists(cx->suite->dir, "") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
drop_test_reopen(stc *cx ssunused)
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
	t( sp_set(c, "db.test.sync", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	sp_destroy(db); /* unref */
	t( sp_drop(db) == 0); /* scheduler drop */
	t( exists(cx->suite->dir, "") == 1 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == -1 ); /* database dropped */
	t( sp_destroy(env) == 0 );
}

stgroup *drop_group(void)
{
	stgroup *group = st_group("drop");
	st_groupadd(group, st_test("test", drop_test));
	st_groupadd(group, st_test("test_reopen", drop_test_reopen));
	return group;
}
