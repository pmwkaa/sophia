
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
repository_empty(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists(st_r.conf->sophia_dir, "log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists(st_r.conf->sophia_dir, "log") == 0 );
	t( exists(st_r.conf->log_dir, "") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists(st_r.conf->sophia_dir, "log") == 1 );
	t( exists(st_r.conf->sophia_dir, "test") == 1 );
	t( exists(st_r.conf->log_dir, "") == 0 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test3(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists(st_r.conf->sophia_dir, "log") == 1 );
	t( exists(st_r.conf->sophia_dir, "test") == 0 );
	t( exists(st_r.conf->db_dir, "") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test4(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "sophia.path_create", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->sophia_dir, "") == 0 );

	mkdir(st_r.conf->sophia_dir, 0755);
	env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "sophia.path_create", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists(st_r.conf->sophia_dir, "test") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test5(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->sophia_dir, "") == 1 );
	t( exists(st_r.conf->sophia_dir, "test") == 1 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.path_fail_on_exists", 1) == 0 );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );
}

stgroup *repository_group(void)
{
	stgroup *group = st_group("repository");
	st_groupadd(group, st_test("empty", repository_empty));
	st_groupadd(group, st_test("test0", repository_test0));
	st_groupadd(group, st_test("test1", repository_test1));
	st_groupadd(group, st_test("test2", repository_test2));
	st_groupadd(group, st_test("test3", repository_test3));
	st_groupadd(group, st_test("test4", repository_test4));
	st_groupadd(group, st_test("test5_fail_on_exists", repository_test5));
	return group;
}
