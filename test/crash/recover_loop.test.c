
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
recover_loop(void)
{
	int seedprev = -1;
	int seed = 3424118;
	int seedorigin = seed;
	int run = 10;
	int count = 1040;

	while (run >= 0) {
		void *env = sp_env();
		t( env != NULL );
		t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
		t( sp_setint(env, "scheduler.threads", 0) == 0 );
		t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
		t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
		t( sp_setstring(env, "db", "test", 0) == 0 );
		t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
		t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
		t( sp_setint(env, "db.test.sync", 0) == 0 );
		t( sp_setint(env, "log.sync", 0) == 0 );
		t( sp_setint(env, "log.rotate_sync", 0) == 0 );
		void *db = sp_getobject(env, "db.test");
		t( db != NULL );
		t( sp_open(env) == 0 );
	
		int i = 0;
		if (seedprev != -1) {
			srand(seedprev);
			while (i < count) {
				int k = rand();
				void *o = sp_object(db);
				t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
				o = sp_get(db, o);
				t( o != NULL );
				t( *(int*)sp_getstring(o, "value", NULL) == k );
				sp_destroy(o);
				i++;
			}
			t( sp_setint(env, "db.test.branch", 0) == 0 );
		}

		srand(seed);
		i = 0;
		while (i < count) {
			int k = rand();
			void *o = sp_object(db);
			t( o != NULL );
			t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
			t( sp_setstring(o, "value", &k, sizeof(k)) == 0 );
			t( sp_set(db, o) == 0 );
			i++;
		}
		t( sp_destroy(env) == 0 );

		seedprev = seed;
		seed = time(NULL);
		run--;
	}

	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	srand(seedorigin);
	int i = 0;
	while (i < count) {
		int k = rand();
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(db, o);
		t( o != NULL );
		t( *(int*)sp_getstring(o, "value", NULL) == k );
		sp_destroy(o);
		i++;
	}
	t( sp_destroy(env) == 0 );
}

stgroup *recover_loop_group(void)
{
	stgroup *group = st_group("recover_loop");
	st_groupadd(group, st_test("test0", recover_loop));
	return group;
}
