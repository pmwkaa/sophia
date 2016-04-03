
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
expire_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.expire", 1) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_getint(env, "db.test.index.count") == 100 );
	sleep(1);
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "db.test.index.count") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
expire_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.expire", 1) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_getint(env, "db.test.index.count") == 100 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "db.test.index.count") == 100 );
	sleep(1);
	t( sp_setint(env, "db.test.compact", 0) == 0 );
	t( sp_getint(env, "db.test.index.count") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
expire_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.expire", 1) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.expire", 1) == 0 );
	t( sp_open(env) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );

	t( sp_getint(env, "db.test.index.count") == 100 );
	sleep(1);
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );
	t( sp_getint(env, "db.test.index.count") == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *expire_group(void)
{
	stgroup *group = st_group("expire");
	st_groupadd(group, st_test("on_branch", expire_test0));
	st_groupadd(group, st_test("on_compact", expire_test1));
	st_groupadd(group, st_test("after_recover", expire_test2));
	return group;
}
