
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
ddl_precreate(stc *cx ssunused)
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
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
ddl_create_online0(stc *cx ssunused)
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
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
ddl_create_online1(stc *cx ssunused)
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
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
ddl_create_online2(stc *cx ssunused)
{
	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", "logdir") == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "db", "s0") == 0 );
	t( sp_set(c, "db.s0.path", "dir0") == 0 );
	t( sp_set(c, "db.s0.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.s0.sync", "0") == 0 );
	void *s0 = sp_get(c, "db.s0");
	t( s0 != NULL );
	t( sp_open(s0) == 0 );

	int key = 7;
	void *o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 8;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 9;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );

	t( sp_set(c, "db", "s1") == 0 );
	t( sp_set(c, "db.s1.path", "dir1") == 0 );
	t( sp_set(c, "db.s1.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.s1.sync", "0") == 0 );
	void *s1 = sp_get(c, "db.s1");
	t( s0 != NULL );
	t( sp_open(s1) == 0 );

	key = 7;
	o = sp_object(s1);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 8;
	o = sp_object(s1);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 9;
	o = sp_object(s1);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );

	t( sp_destroy(s1) == 0 );
	t( sp_destroy(s0) == 0 );
	t( sp_destroy(env) == 0 );

	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");
}

static void
ddl_open_online0(stc *cx ssunused)
{
	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", "logdir") == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "db", "s0") == 0 );
	t( sp_set(c, "db.s0.path", "dir0") == 0 );
	t( sp_set(c, "db.s0.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.s0.sync", "0") == 0 );
	void *s0 = sp_get(c, "db.s0");
	t( s0 != NULL );
	t( sp_open(s0) == 0 );

	int key = 7;
	void *o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 8;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 9;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	t( sp_destroy(s0) == 0 );
	t( sp_destroy(s0) == 0 ); /* shutdown */
#if 0
	t( sp_set(c, "scheduler.run") == 1 ); /* proceed shutdown */
#endif

	t( sp_set(c, "db", "s0") == 0 );
	t( sp_set(c, "db.s0.path", "dir0") == 0 );
	t( sp_set(c, "db.s0.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.s0.sync", "0") == 0 );
	/* ban open existing databases */
	s0 = sp_get(c, "db.s0");
	t( s0 != NULL );
	t( sp_open(s0) == -1 );

	t( sp_destroy(env) == 0 );

	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");
}

stgroup *ddl_group(void)
{
	stgroup *group = st_group("ddl");
	st_groupadd(group, st_test("precreate", ddl_precreate));
	st_groupadd(group, st_test("create_online0", ddl_create_online0));
	st_groupadd(group, st_test("create_online1", ddl_create_online1));
	st_groupadd(group, st_test("create_online2", ddl_create_online2));
	st_groupadd(group, st_test("open_online0", ddl_open_online0));
	return group;
}
