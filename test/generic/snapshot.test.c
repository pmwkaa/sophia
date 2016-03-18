
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
snapshot_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 0 );

	t( sp_setint(env, "scheduler.snapshot", 0) == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 1 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 1 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 0 );

	int rc;
	while ( (rc = sp_setint(env, "scheduler.run", 0)) > 0 );
	t( rc == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 1 );

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( sp_open(env) == 0 );

	key = 0;
	while (key < 10) {
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

static void
snapshot_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 0;
	while (key < 10) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 0 );

	t( sp_setint(env, "scheduler.snapshot", 0) == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 1 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 1 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 0 );

	int rc;
	while ( (rc = sp_setint(env, "scheduler.run", 0)) > 0 );
	t( rc == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 1 );

	while (key < 15) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( sp_open(env) == 0 );

	key = 0;
	while (key < 10) {
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

static void
snapshot_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "t0", 0) == 0 );
	t( sp_setstring(env, "db", "t1", 0) == 0 );
	t( sp_setstring(env, "db.t0.index.key", "u32", 0) == 0 );
	t( sp_setstring(env, "db.t1.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.t0.sync", 0) == 0 );
	t( sp_setint(env, "db.t1.sync", 0) == 0 );

	void *t0 = sp_getobject(env, "db.t0");
	t( t0 != NULL );
	void *t1 = sp_getobject(env, "db.t1");
	t( t1 != NULL );
	t( sp_open(env) == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 0 );

	t( sp_setint(env, "scheduler.snapshot", 0) == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 1 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 1 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 0 );

	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_getint(env, "scheduler.snapshot_active") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn") == 0 );
	t( sp_getint(env, "scheduler.snapshot_ssn_last") == 1 );

	t( sp_destroy(env) == 0 );
}

stgroup *snapshot_group(void)
{
	stgroup *group = st_group("snapshot");
	st_groupadd(group, st_test("create_and_recover", snapshot_test0));
	st_groupadd(group, st_test("invalidate", snapshot_test1));
	st_groupadd(group, st_test("md", snapshot_test2));
	return group;
}
