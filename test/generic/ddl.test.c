
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
ddl_precreate(void)
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
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
ddl_create_online0(void)
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
	t( sp_destroy(env) == 0 );
}

static void
ddl_create_online1(void)
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
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
ddl_create_online2(void)
{
	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");

	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "s0", 0) == 0 );
	t( sp_setstring(env, "db.s0.path", "dir0", 0) == 0 );
	t( sp_setstring(env, "db.s0.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.s0.sync", 0) == 0 );
	void *s0 = sp_getobject(env, "db.s0");
	t( s0 != NULL );
	t( sp_open(s0) == 0 );

	int key = 7;
	void *o = sp_object(s0);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 8;
	o = sp_object(s0);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 9;
	o = sp_object(s0);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );

	t( sp_setstring(env, "db", "s1", 0) == 0 );
	t( sp_setstring(env, "db.s1.path", "dir1", 0) == 0 );
	t( sp_setstring(env, "db.s1.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.s1.sync", 0) == 0 );
	void *s1 = sp_getobject(env, "db.s1");
	t( s1 != NULL );
	t( sp_open(s1) == 0 );

	key = 7;
	o = sp_object(s1);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 8;
	o = sp_object(s1);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 9;
	o = sp_object(s1);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );

	t( sp_destroy(s1) == 0 );
	t( sp_destroy(s0) == 0 );
	t( sp_destroy(env) == 0 );

	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");
}

static void
ddl_open_online0(void)
{
	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");

	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "s0", 0) == 0 );
	t( sp_setstring(env, "db.s0.path", "dir0", 0) == 0 );
	t( sp_setstring(env, "db.s0.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.s0.sync", 0) == 0 );
	void *s0 = sp_getobject(env, "db.s0");
	t( s0 != NULL );
	t( sp_open(s0) == 0 );

	int key = 7;
	void *o = sp_object(s0);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 8;
	o = sp_object(s0);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	key = 9;
	o = sp_object(s0);
	sp_setstring(o, "key", &key, sizeof(key));
	t( sp_set(s0, o) == 0 );
	t( sp_destroy(s0) == 0 );
	t( sp_destroy(s0) == 0 ); /* shutdown */

	t( sp_setstring(env, "db", "s0", 0) == 0 );
	t( sp_setstring(env, "db.s0.path", "dir0", 0) == 0 );
	t( sp_setstring(env, "db.s0.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.s0.sync", 0) == 0 );
	/* ban open existing databases */
	s0 = sp_getobject(env, "db.s0");
	t( s0 != NULL );
	t( sp_open(s0) == -1 );

	t( sp_destroy(env) == 0 );

	rmrf("./logdir");
	rmrf("./dir0");
	rmrf("./dir1");
}

static void
ddl_constraint(void)
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
	t( sp_setstring(env, "db.test.index", "key-2", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key-3", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key-4", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key-5", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key-6", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key-7", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key-8", 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key-9", 0) == -1 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *ddl_group(void)
{
	stgroup *group = st_group("ddl");
	st_groupadd(group, st_test("precreate", ddl_precreate));
	st_groupadd(group, st_test("create_online0", ddl_create_online0));
	st_groupadd(group, st_test("create_online1", ddl_create_online1));
	st_groupadd(group, st_test("create_online2", ddl_create_online2));
	st_groupadd(group, st_test("open_online0", ddl_open_online0));
	st_groupadd(group, st_test("constraint", ddl_constraint));
	return group;
}
