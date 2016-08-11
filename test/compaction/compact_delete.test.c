
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
compact_delete_node0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.branch_wm", 1) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
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
	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
compact_delete_node1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.branch_wm", 1) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.node_size", 716800/* 700K */) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	char value[100];
	memset(value, 0, sizeof(value));
	while (key < 13000) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.node_count") == 2 );

	key = 0;
	while (key < 6511 ) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}

	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.node_count") == 1 );

	void *o = sp_document(db);
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		int keysize;
		void *keyptr = sp_getstring(o, "key", &keysize);
		t( *(uint32_t*)keyptr == key );
		void *ko = sp_document(db);
		t( ko != NULL );
		t( sp_setstring(ko, "key", keyptr, keysize) == 0 );
		t( sp_delete(db, ko) == 0 );
		key++;
	}
	sp_destroy(cur);
	t( key == 13000 );

	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.node_count") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
compact_delete0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.branch_wm", 1) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
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

	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	t( o != NULL );
	int i = 0;
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( i == 0 );

	t( sp_destroy(env) == 0 );
}

static void
compact_delete1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.branch_wm", 1) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
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

	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}

	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	t( o != NULL );
	int i = 0;
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( i == 0 );

	t( sp_destroy(env) == 0 );
}

static void
compact_delete_cursor(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.branch_wm", 1) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 25) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}
	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	t( o != NULL );
	int i = 0;
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == 20 + i );
		i++;
	}
	t( i == 5 );

	t( sp_destroy(env) == 0 );
}

stgroup *compact_delete_group(void)
{
	stgroup *group = st_group("compact_delete");
	st_groupadd(group, st_test("node0", compact_delete_node0));
	st_groupadd(group, st_test("node1", compact_delete_node1));
	st_groupadd(group, st_test("compaction0", compact_delete0));
	st_groupadd(group, st_test("compaction1", compact_delete1));
	st_groupadd(group, st_test("cursor", compact_delete_cursor));
	return group;
}
