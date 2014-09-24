
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <sophia.h>

#include <time.h>
#include <assert.h>
#include <sys/time.h>
#include "suite.h"

static void
test_precreate(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );

	void *s = sp_storage(env);
	t( s != NULL );
	void *c = sp_ctl(s, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_create_online0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );

	void *s = sp_storage(env);
	t( s != NULL );
	void *c = sp_ctl(s, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
	t( sp_open(s) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_create_online1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );

	void *s = sp_storage(env);
	t( s != NULL );
	void *c = sp_ctl(s, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
	t( sp_open(s) == 0 );
	t( sp_destroy(s) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_create_online2(void)
{
	rmrf("./test");
	rmrf("./log");

	rmrf("./test0");
	rmrf("./log0");
	rmrf("./test1");
	rmrf("./log1");

	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );

	void *s0 = sp_storage(env);
	t( s0 != NULL );
	void *c = sp_ctl(s0, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log0") == 0 );
	t( sp_set(c, "storage.dir", "test0") == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
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

	void *s1 = sp_storage(env);
	t( s1 != NULL );
	c = sp_ctl(s1, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log1") == 0 );
	t( sp_set(c, "storage.dir", "test1") == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
	t( sp_open(s1) == 0 );

	key = 7;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 8;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 9;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );

	t( sp_destroy(s1) == 0 );
	t( sp_destroy(s0) == 0 );
	t( sp_destroy(env) == 0 );

	rmrf("./test0");
	rmrf("./log0");
	rmrf("./test1");
	rmrf("./log1");
}

static void
test_create_online3(void)
{
	rmrf("./test");
	rmrf("./log");

	rmrf("./test0");
	rmrf("./log0");
	rmrf("./test1");
	rmrf("./log1");

	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );

	void *s0 = sp_storage(env);
	t( s0 != NULL );
	void *c = sp_ctl(s0, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log0") == 0 );
	t( sp_set(c, "storage.dir", "test0") == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
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

	void *s1 = sp_storage(env);
	t( s1 != NULL );
	c = sp_ctl(s1, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log1") == 0 );
	t( sp_set(c, "storage.dir", "test1") == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
	t( sp_open(s1) == 0 );

	key = 7;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 8;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	key = 9;
	o = sp_object(s0);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(s1, o) == 0 );
	t( sp_destroy(env) == 0 );

	rmrf("./test0");
	rmrf("./log0");
	rmrf("./test1");
	rmrf("./log1");
}

int
main(int argc, char *argv[])
{
	test( test_precreate );
	test( test_create_online0 );
	test( test_create_online1 );
	test( test_create_online2 );
	test( test_create_online3 );
	return 0;
}
