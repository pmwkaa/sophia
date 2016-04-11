
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
compact_branch(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "compaction.0.compact_mode", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
compact_temperature(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "compaction.0.compact_mode", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.temperature", 1) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
compact_index(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "compaction.0.mode", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.compact_index", 0) == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		o = sp_get(db, o);
		t( o != NULL );
		sp_destroy(o);
		key++;
	}

	t( sp_destroy(env) == 0 );
}

stgroup *compact_group(void)
{
	stgroup *group = st_group("compact");
	st_groupadd(group, st_test("by_branch", compact_branch));
	st_groupadd(group, st_test("by_temperature", compact_temperature));
	st_groupadd(group, st_test("scheme", compact_index));
	return group;
}
