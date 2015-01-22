
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
repository_empty(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test0(stc *cx srunused)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", "sophia") == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test1(stc *cx srunused)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", "sophia") == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", "logdir") == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 0 );
	t( exists("logdir", "") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test2(stc *cx srunused)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", "sophia") == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 1 );
	t( exists("sophia", "test") == 1 );
	t( exists("logdir", "") == 0 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test3(stc *cx srunused)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", "sophia") == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", "dir") == 0 );
	t( sp_open(env) == 0 );
	t( exists("sophia", "log") == 1 );
	t( exists("sophia", "test") == 0 );
	t( exists("dir", "") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
repository_test4(stc *cx srunused)
{
	rmrf("./sophia");
	rmrf("./logdir");
	rmrf("./dir");
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", "sophia") == 0 );
	t( sp_set(c, "sophia.path_create", "0") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	t( exists("sophia", "") == 0 );

	mkdir("sophia", 0755);
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", "sophia") == 0 );
	t( sp_set(c, "sophia.path_create", "0") == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
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
