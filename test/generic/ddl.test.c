
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
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
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
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test2", 0) == -1 );
	t( sp_destroy(env) == 0 );
}

static void
ddl_constraint(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-1", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-2", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-3", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-4", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-5", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-6", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-7", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-8", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key-9", 0) == -1 );
	t( sp_setstring(env, "db.test.scheme.key-1", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *ddl_group(void)
{
	stgroup *group = st_group("ddl");
	st_groupadd(group, st_test("precreate", ddl_precreate));
	st_groupadd(group, st_test("create_online0", ddl_create_online0));
	st_groupadd(group, st_test("constraint", ddl_constraint));
	return group;
}
