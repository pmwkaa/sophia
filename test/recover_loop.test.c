
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
recover_loop(stc *cx)
{
	int seedprev = -1;
	int seed = 3424118;
	int seedorigin = seed;
	int run = 100;
	int count = 1040;

	while (run >= 0) {
		cx->env = sp_env();
		t( cx->env != NULL );
		void *c = sp_ctl(cx->env);
		t( c != NULL );
		t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
		t( sp_set(c, "scheduler.threads", "0") == 0 );
		t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
		t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
		t( sp_set(c, "log.sync", "0") == 0 );
		t( sp_set(c, "log.rotate_sync", "0") == 0 );
		t( sp_set(c, "db", "test") == 0 );
		t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
		t( sp_set(c, "db.test.sync", "0") == 0 );
		t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
		cx->db = sp_get(c, "db.test");
		t( cx->db != NULL );
		t( sp_open(cx->env) == 0 );
	
		int i = 0;
		if (seedprev != -1) {
			srand(seedprev);
			while (i < count) {
				int k = rand();
				void *o = sp_object(cx->db);
				t( sp_set(o, "key", &k, sizeof(k)) == 0 );
				o = sp_get(cx->db, o);
				t( o != NULL );
				st_transaction(cx);
				t( *(int*)sp_get(o, "value", NULL) == k );
				sp_destroy(o);
				i++;
			}
			t( sp_set(c, "db.test.branch") == 0 );
		}

		srand(seed);
		i = 0;
		while (i < count) {
			int k = rand();
			void *o = sp_object(cx->db);
			t( o != NULL );
			t( sp_set(o, "key", &k, sizeof(k)) == 0 );
			t( sp_set(o, "value", &k, sizeof(k)) == 0 );
			t( sp_set(cx->db, o) == 0 );
			st_transaction(cx);
			i++;
		}
		t( sp_destroy(cx->env) == 0 );

		seedprev = seed;
		seed = time(NULL);
		run--;
	}

	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	srand(seedorigin);
	int i = 0;
	while (i < count) {
		int k = rand();
		void *o = sp_object(cx->db);
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(cx->db, o);
		t( o != NULL );
		t( *(int*)sp_get(o, "value", NULL) == k );
		sp_destroy(o);
		i++;
	}
	t( sp_destroy(cx->env) == 0 );
	cx->env = NULL;
	cx->db  = NULL;
}

stgroup *recoverloop_group(void)
{
	stgroup *group = st_group("recover_loop");
	st_groupadd(group, st_test("test", recover_loop));
	return group;
}
