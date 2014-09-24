
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
test_recoverloop(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env;
	void *db;
	void *c;

	int seedprev = -1;
	int seed = 3424118;
	int seedorigin = seed;
	int run = 100;
	int count = 10040;

	while (run >= 0) {
		env = sp_env();
		t( env != NULL );
		db = sp_storage(env);
		t( db != NULL );
		c = sp_ctl(db, "conf");
		t( c != NULL );
		t( sp_set(c, "storage.logdir", "log") == 0 );
		t( sp_set(c, "storage.dir", "test") == 0 );
		t( sp_open(env) == 0 );
	
		int i = 0;
		if (seedprev != -1) {
			srand(seedprev);
			while (i < count) {
				int k = rand();
				void *o = sp_object(db);
				t( sp_set(o, "key", &k, sizeof(k)) == 0 );
				o = sp_get(db, o);
				t( o != NULL );
				t( *(int*)sp_get(o, "value", NULL) == k );
				sp_destroy(o);
				i++;
			}
		}

		srand(seed);
		i = 0;
		while (i < count) {
			int k = rand();
			void *o = sp_object(db);
			t( o != NULL );
			t( sp_set(o, "key", &k, sizeof(k)) == 0 );
			t( sp_set(o, "value", &k, sizeof(k)) == 0 );
			t( sp_set(db, o) == 0 );
			i++;
		}
		t( sp_destroy(env) == 0 );

		seedprev = seed;
		seed = time(NULL);
		run--;
	}

	env = sp_env();
	t( env != NULL );
	db = sp_storage(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_open(env) == 0 );
	srand(seedorigin);
	int i = 0;
	while (i < count) {
		int k = rand();
		void *o = sp_object(db);
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(db, o);
		t( o != NULL );
		t( *(int*)sp_get(o, "value", NULL) == k );
		sp_destroy(o);
		i++;
	}
	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_recoverloop );
	return 0;
}
