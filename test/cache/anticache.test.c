
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
anticache_promote0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "memory.anticache", 100 * 1024 * 1024) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setstring(env, "db.test.storage", "anti-cache", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.temperature", 1) == 0 );
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

	t( sp_getint(env, "scheduler.anticache_active") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn_last") == 0 );

	t( sp_setint(env, "scheduler.anticache", 0) == 0 );

	t( sp_getint(env, "scheduler.anticache_active") == 1 );
	t( sp_getint(env, "scheduler.anticache_asn") == 1 );
	t( sp_getint(env, "scheduler.anticache_asn_last") == 0 );

	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_getint(env, "scheduler.anticache_active") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn_last") == 1 );

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
anticache_promote1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "memory.anticache", 100 * 1024 * 1024) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setstring(env, "db.test.storage", "anti-cache", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.temperature", 1) == 0 );
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

	t( sp_getint(env, "scheduler.anticache_active") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn_last") == 0 );
	t( sp_setint(env, "scheduler.anticache", 0) == 0 );
	t( sp_getint(env, "scheduler.anticache_active") == 1 );
	t( sp_getint(env, "scheduler.anticache_asn") == 1 );
	t( sp_getint(env, "scheduler.anticache_asn_last") == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
	t( sp_getint(env, "scheduler.anticache_active") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn") == 0 );
	t( sp_getint(env, "scheduler.anticache_asn_last") == 1 );

	t( sp_setint(env, "scheduler.anticache", 0) == 0 );
	t( sp_getint(env, "scheduler.anticache_active") == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *anticache_group(void)
{
	stgroup *group = st_group("anticache");
	st_groupadd(group, st_test("promote0", anticache_promote0));
	st_groupadd(group, st_test("promote1", anticache_promote1));
	return group;
}
