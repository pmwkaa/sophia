
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
leak_set(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 124;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_set_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_getint(env, "performance.documents") == 1 );

	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( sp_getint(env, "performance.documents") == 1 ); /* reuse same object */
	sp_destroy(o);
	t( sp_getint(env, "performance.documents") == 1 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_set_commit(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *b = sp_begin(env);
	t( b != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	key = 124;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );

	t( sp_commit(b) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_set_get_commit(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *b = sp_begin(env);
	t( b != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	key = 1;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o == NULL );

	t( sp_getint(env, "performance.documents") == 2 );

	t( sp_commit(b) == 0 );

	t( sp_getint(env, "performance.documents") == 1 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_set_get_rollback(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *b = sp_begin(env);
	t( b != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	key = 1;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o == NULL );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_destroy(b) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_tx_set_commit(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *a = sp_begin(env);
	t( a != NULL );

	void *b = sp_begin(env);
	t( b != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	key = 124;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );

	t( sp_commit(b) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_commit(a) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_tx_set_rollback(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *a = sp_begin(env);
	t( a != NULL );

	void *b = sp_begin(env);
	t( b != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	key = 124;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );

	t( sp_destroy(b) == 0 );

	t( sp_getint(env, "performance.documents") == 0 );
	t( sp_commit(a) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_tx_set_get_commit0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *a = sp_begin(env);
	t( a != NULL );

	void *b = sp_begin(env);
	t( b != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	key = 1;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o == NULL );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_commit(b) == 0 );
	t( sp_getint(env, "performance.documents") == 1 ); /* last read freed */

	t( sp_commit(a) == 0 );
	t( sp_getint(env, "performance.documents") == 1 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_tx_set_get_commit1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *a = sp_begin(env);
	t( a != NULL );

	void *b = sp_begin(env);
	t( b != NULL );

	int key = 1;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o == NULL );

	key = 123;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_commit(b) == 0 );
	t( sp_getint(env, "performance.documents") == 2 );

	t( sp_commit(a) == 0 );
	t( sp_getint(env, "performance.documents") == 1 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_tx_tx_set_get_commit0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *a = sp_begin(env);
	t( a != NULL );

	void *b = sp_begin(env);
	t( b != NULL );

	void *c = sp_begin(env);
	t( c != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );

	t( sp_getint(env, "performance.documents") == 1 );
	t( sp_commit(a) == 0 );
	t( sp_getint(env, "performance.documents") == 1 );

	key = 124;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_commit(b) == 0 );
	t( sp_getint(env, "performance.documents") == 2 );

	key = 125;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(c, o) == 0 );

	t( sp_getint(env, "performance.documents") == 3 );
	t( sp_commit(c) == 0 );
	t( sp_getint(env, "performance.documents") == 3 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
leak_tx_tx_tx_set_get_commit1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	void *a = sp_begin(env);
	t( a != NULL );

	void *b = sp_begin(env);
	t( b != NULL );

	void *c = sp_begin(env);
	t( c != NULL );

	int key = 123;
	void *o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(c, o) == 0 );

	t( sp_getint(env, "performance.documents") == 1 );
	t( sp_commit(c) == 0 );
	t( sp_getint(env, "performance.documents") == 1 );

	key = 124;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_getint(env, "performance.documents") == 2 );
	t( sp_commit(b) == 0 );
	t( sp_getint(env, "performance.documents") == 2 );

	key = 125;
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );

	t( sp_getint(env, "performance.documents") == 3 );
	t( sp_commit(a) == 0 );
	t( sp_getint(env, "performance.documents") == 3 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_getint(env, "performance.documents") == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *leak_group(void)
{
	stgroup *group = st_group("leak");
	st_groupadd(group, st_test("set", leak_set));
	st_groupadd(group, st_test("set_get", leak_set_get));
	st_groupadd(group, st_test("tx_set_commit", leak_tx_set_commit));
	st_groupadd(group, st_test("tx_set_get_commit", leak_tx_set_get_commit));
	st_groupadd(group, st_test("tx_set_get_rollback", leak_tx_set_get_rollback));
	st_groupadd(group, st_test("tx_tx_set_commit", leak_tx_tx_set_commit));
	st_groupadd(group, st_test("tx_tx_set_rollback", leak_tx_tx_set_rollback));
	st_groupadd(group, st_test("tx_tx_set_get_commit0", leak_tx_tx_set_get_commit0));
	st_groupadd(group, st_test("tx_tx_set_get_commit1", leak_tx_tx_set_get_commit1));
	st_groupadd(group, st_test("tx_tx_tx_set_get_commit0", leak_tx_tx_tx_set_get_commit0));
	st_groupadd(group, st_test("tx_tx_tx_set_get_commit1", leak_tx_tx_tx_set_get_commit1));
	return group;
}
