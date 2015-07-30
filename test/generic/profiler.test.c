
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
profiler_count(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	t( sp_getint(env, "db.test.index.branch_count") == 1 );
	t( sp_getint(env, "db.test.index.node_count") == 1 );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 100 );
	t( sp_getint(env, "db.test.index.branch_count") == 2 );

	i = 0;
	while ( i < 10 ) {
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_getint(env, "db.test.index.count") == 110 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );
	t( sp_getint(env, "db.test.index.count") == 100 );

	t( sp_destroy(env) == 0 );
}

stgroup *profiler_group(void)
{
	stgroup *group = st_group("profiler");
	st_groupadd(group, st_test("count", profiler_count));
	return group;
}
