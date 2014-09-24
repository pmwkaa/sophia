
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <sophia.h>
#include "suite.h"

static void
test_object_db(void)
{
	rmrf("./test");
	rmrf("./log");
	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	void *o = sp_object(db);
	t(o != NULL);
	sp_destroy(o);
	o = sp_object(env);
	t(o == NULL);
	t( sp_destroy(env) == 0 );
}

static void
test_object_setget(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	int size = 0;
	t( *(int*)sp_get(o, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_get(o, "value", &size) == key );
	t( size == sizeof(key) );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
test_object_copy0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );

	void *copy = sp_copy(o);
	t( copy != NULL );
	int size = 0;
	t( *(int*)sp_get(copy, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_get(copy, "value", &size) == key );
	t( size == sizeof(key) );

	sp_destroy(o);
	sp_destroy(copy);

	t( sp_destroy(env) == 0 );
}

static void
test_object_copy1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	void *c = sp_cursor(db, ">", NULL);
	o = sp_get(c);
	t( o != NULL );
	void *copy = sp_copy(o);
	t( copy != NULL );
	o = sp_get(c);
	t( o ==  NULL );
	sp_destroy(c);

	int size = 0;
	t( *(int*)sp_get(copy, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_get(copy, "value", &size) == key );
	t( size == sizeof(key) );
	sp_destroy(copy);

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_object_db );
	test( test_object_setget );
	test( test_object_copy0 );
	test( test_object_copy1 );
	return 0;
}
