
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
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", "sophia", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test1(void)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", "sophia", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", "logdir", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 0 );
	t( exists("logdir", "") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test2(void)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", "sophia", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 1 );
	t( exists("sophia", "test") == 1 );
	t( exists("logdir", "") == 0 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test3(void)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", "sophia", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", "dir", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 1 );
	t( exists("sophia", "test") == 0 );
	t( exists("dir", "") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test4(void)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", "sophia", 0) == 0 );
	t( sp_setint(env, "sophia.path_create", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists("sophia", "") == 0 );

	mkdir("sophia", 0755);
	env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", "sophia", 0) == 0 );
	t( sp_setint(env, "sophia.path_create", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "test") == 1 );
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
	return group;
}
