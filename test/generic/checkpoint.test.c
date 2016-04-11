
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
checkpoint_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
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

	t( sp_getint(env, "scheduler.checkpoint_active") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn_last") == 0 );

	t( sp_setint(env, "scheduler.checkpoint", 0) == 0 );

	t( sp_getint(env, "scheduler.checkpoint_active") == 1 );
	t( sp_getint(env, "scheduler.checkpoint_lsn") == 10 );
	t( sp_getint(env, "scheduler.checkpoint_lsn_last") == 0 );

	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_getint(env, "scheduler.checkpoint_active") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn_last") == 10 );

	t( sp_destroy(env) == 0 );
}

static void
checkpoint_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	int key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	t( sp_setint(env, "log.rotate", 0) == 0 );
	t( sp_getint(env, "log.files") == 2 );

	key = 40;
	while (key < 80) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	t( sp_setint(env, "log.rotate", 0) == 0 );
	t( sp_getint(env, "log.files") == 3 );

	t( sp_setint(env, "scheduler.checkpoint", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_getint(env, "log.files") == 1 );

	t( sp_destroy(env) == 0 );
}

static void
checkpoint_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_getint(env, "scheduler.checkpoint_active") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn_last") == 0 );

	t( sp_setint(env, "scheduler.checkpoint", 0) == 0 );

	t( sp_getint(env, "scheduler.checkpoint_active") == 1 );
	t( sp_getint(env, "scheduler.checkpoint_lsn") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn_last") == 0 );

	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_getint(env, "scheduler.checkpoint_active") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn") == 0 );
	t( sp_getint(env, "scheduler.checkpoint_lsn_last") == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *checkpoint_group(void)
{
	stgroup *group = st_group("checkpoint");
	st_groupadd(group, st_test("test0", checkpoint_test0));
	st_groupadd(group, st_test("test1", checkpoint_test1));
	st_groupadd(group, st_test("test2", checkpoint_test2));
	return group;
}
