
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
test0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );

	void *s = sp_storage(env);
	t( s != NULL );
	void *c = sp_ctl(s, "conf");
	t( c != NULL );
	int indexsize = 1000 * (sizeof(int) * 2 + sizeof(svv));
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.node_branch_wm", indexsize) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 1000) {
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	while (i < 2000) {
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	while (i < 8000) {
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );

	void *s = sp_storage(env);
	t( s != NULL );
	void *c = sp_ctl(s, "conf");
	t( c != NULL );
	int indexsize = 1000 * (sizeof(int) * 2 + sizeof(svv));
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.node_branch_wm", indexsize) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	srand(12341531);

	int i = 0;
	while (i < 1000) {
		int k = rand();
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &k, sizeof(k)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	while (i < 10000) {
		int k = rand();
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &k, sizeof(k)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	while (i < 15000) {
		int k = rand();
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &k, sizeof(k)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );

	void *s = sp_storage(env);
	t( s != NULL );
	void *c = sp_ctl(s, "conf");
	t( c != NULL );
	int indexsize = 1000 * (sizeof(int) * 2 + sizeof(svv));
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.node_branch_wm", indexsize) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	srand(12341531);

	int i = 0;
	while (i < 1000) {
		int k = rand();
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &k, sizeof(k)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	i = 0;
	while (i < 1000) {
		int k = rand();
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &k, sizeof(k)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	i = 0;
	while (i < 1000) {
		int k = rand();
		void *o = sp_object(s);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &k, sizeof(k)) == 0 );
		t( sp_set(s, o) == 0 );
		i++;
	}
	t( sp_set(sp_ctl(s, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(s, "ctl"), "merge") == 0 );

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test0 );
	test( test1 );
	test( test2 );
	return 0;
}
