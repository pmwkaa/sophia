
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
durability_deploy0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_setint(env, "debug.error_injection.si_recover_0", 1) == 0 );
	t( sp_open(env) == -1 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000000.00000000000000000001.db.incomplete") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000000.00000000000000000001.db.incomplete") == 0 );

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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 ); /* reuse empty directory */
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000000.00000000000000000001.db.incomplete") == 0 );
}

static void
durability_branch0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 7;
	void *o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_setint(env, "debug.error_injection.si_branch_0", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	o = sp_get(c, o);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
}

static void
durability_build0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 7;
	void *o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_setint(env, "debug.error_injection.sd_build_0", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	o = sp_get(c, o);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
}

static void
durability_build1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 7;
	void *o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_setint(env, "debug.error_injection.sd_build_1", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 ); /* seal crc is corrupted */
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	o = sp_get(c, o);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
}

static void
durability_compact0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 7;
	void *o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_setint(env, "debug.error_injection.si_compaction_0", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	o = sp_get(c, o);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
}

static void
durability_compact1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 7;
	void *o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_setint(env, "debug.error_injection.si_compaction_1", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	o = sp_get(c, o);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000003.db") == 1 );
}

static void
durability_compact2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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

	int key = 7;
	void *o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_document(db);
	t( o != 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_setint(env, "debug.error_injection.si_compaction_2", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 7 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 8 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "key", NULL) == 9 );
	o = sp_get(c, o);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000003.db") == 1 );
}

static void
durability_compact3(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.node_size", 60) == 0 );
	t( sp_setint(env, "db.test.page_size", 60) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 20) {
		void *o = sp_document(db);
		t( o != 0 );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "debug.error_injection.si_compaction_0", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 0 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	i = 0;
	while ((o = sp_get(c, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 0 );
}

static void
durability_compact4(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.node_size", 45) == 0 );
	t( sp_setint(env, "db.test.page_size", 45) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 20) {
		void *o = sp_document(db);
		t( o != 0 );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "debug.error_injection.si_compaction_1", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	i = 0;
	while ((o = sp_get(c, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000003.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000004.db") == 1 );
}

static void
durability_compact5(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.node_size", 45) == 0 );
	t( sp_setint(env, "db.test.page_size", 45) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 20) {
		void *o = sp_document(db);
		t( o != 0 );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "debug.error_injection.si_compaction_2", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	i = 0;
	while ((o = sp_get(c, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000003.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000004.db") == 1 );
}

static void
durability_compact6(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.node_size", 45) == 0 );
	t( sp_setint(env, "db.test.page_size", 45) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 20) {
		void *o = sp_document(db);
		t( o != 0 );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "debug.error_injection.si_compaction_3", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 0 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	i = 0;
	while ((o = sp_get(c, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000003.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000004.db") == 0 );
}

static void
durability_compact7(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.node_size", 45) == 0 );
	t( sp_setint(env, "db.test.page_size", 45) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 20) {
		void *o = sp_document(db);
		t( o != 0 );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "debug.error_injection.si_compaction_4", 1) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000003.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(env);
	t( c != NULL );
	i = 0;
	while ((o = sp_get(c, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "00000000000000000001.db") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000003.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000001.00000000000000000004.db.seal") == 0 );
	t( exists(st_r.conf->db_dir, "00000000000000000003.db") == 1 );
	t( exists(st_r.conf->db_dir, "00000000000000000004.db") == 1 );
}

static void
durability_snapshot0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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
	t( sp_setint(env, "debug.error_injection.si_snapshot_0", 1) == 0 );

	t( sp_setint(env, "scheduler.snapshot", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == -1 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "index.incomplete") == 1 );
	t( exists(st_r.conf->db_dir, "index") == 0 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "index.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "index") == 0 );
}

static void
durability_snapshot1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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
	t( sp_setint(env, "debug.error_injection.si_snapshot_1", 1) == 0 );

	t( sp_setint(env, "scheduler.snapshot", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == -1 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "index.incomplete") == 1 );
	t( exists(st_r.conf->db_dir, "index") == 0 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "index.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "index") == 0 );
}

static void
durability_snapshot2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
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
	t( sp_setint(env, "debug.error_injection.si_snapshot_2", 1) == 0 );

	t( sp_setint(env, "scheduler.snapshot", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == -1 );

	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "index.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "index") == 1 );

	/* recover */
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
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	t( exists(st_r.conf->db_dir, "index.incomplete") == 0 );
	t( exists(st_r.conf->db_dir, "index") == 1 );
}

stgroup *durability_group(void)
{
	stgroup *group = st_group("durability");
	st_groupadd(group, st_test("deploy_case0",  durability_deploy0));
	st_groupadd(group, st_test("branch_case0",  durability_branch0));
	st_groupadd(group, st_test("build_case0",   durability_build0));
	st_groupadd(group, st_test("build_case1",   durability_build1));
	st_groupadd(group, st_test("compact_case0", durability_compact0));
	st_groupadd(group, st_test("compact_case1", durability_compact1));
	st_groupadd(group, st_test("compact_case2", durability_compact2));
	st_groupadd(group, st_test("compact_case3", durability_compact3));
	st_groupadd(group, st_test("compact_case4", durability_compact4));
	st_groupadd(group, st_test("compact_case5", durability_compact5));
	st_groupadd(group, st_test("compact_case6", durability_compact6));
	st_groupadd(group, st_test("compact_case7", durability_compact7));
	st_groupadd(group, st_test("snapshot_case0", durability_snapshot0));
	st_groupadd(group, st_test("snapshot_case1", durability_snapshot1));
	st_groupadd(group, st_test("snapshot_case2", durability_snapshot2));
	return group;
}
