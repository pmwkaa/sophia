
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
test0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.a.logdir", "log") == 0 );
	t( sp_set(c, "db.a.dir", "test") == 0 );
	t( sp_open(env) == 0 );
	void *a = sp_get(c, "db.a");
	t( a != NULL );
	t( sp_set(c, "db.a.branch") == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );

	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.a") == 0 );
	t( sp_set(c, "db.a.logdir", "log") == 0 );
	t( sp_set(c, "db.a.dir", "test") == 0 );
	void *a = sp_get(c, "db.a");
	t( a != NULL );
	t( sp_open(a) == 0 );

	int key = 7;
	void *o = sp_object(a);
	sp_set(o, "key", &key, sizeof(key));
	t ( sp_set(a, o) == 0 );
	t( sp_destroy(a) == 0 );

	a = sp_get(c, "db.a");
	t( a == NULL );

	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.a") == 0 );
	t( sp_set(c, "db.a.logdir", "log") == 0 );
	t( sp_set(c, "db.a.dir", "test") == 0 );
	a = sp_get(c, "db.a");
	t( a != NULL );
	t( sp_open(a) == 0 );

	o = sp_object(a);
	sp_set(o, "key", &key);
	o = sp_get(a, o);
	t( o != NULL );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test0 );
	test( test1 );
	return 0;
}
