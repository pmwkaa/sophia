
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
tpr_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "sophia.recover", 2) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	char *v = sp_getstring(env, "db.test.status", NULL);
	t( strcmp(v, "recover") == 0 );
	free(v);

	t( sp_open(env) == 0 ); /* complete */

	v = sp_getstring(env, "db.test.status", NULL);
	t( strcmp(v, "online") == 0 );
	free(v);

	t( sp_open(db) == -1 );

	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
tpr_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "sophia.recover", 2) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	t( sp_open(env) == 0 );

	int key = 7;
	int value = 8;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	key = 7;
	value = 9;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "sophia.recover", 2) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	char *v = sp_getstring(env, "db.test.status", NULL);
	t( strcmp(v, "recover") == 0 );
	free(v);

	void *tx = sp_begin(env);
	t( tx != NULL );
	key = 7;
	value = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_setint(tx, "lsn", 1) == 0 );
	t( sp_commit(tx) == 0 ); /* skip */

	tx = sp_begin(env);
	t( tx != NULL );
	key = 7;
	value = 9;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_setint(tx, "lsn", 2) == 0 );
	t( sp_commit(tx) == 0 ); /* commit */

	t( sp_open(env) == 0 );

	v = sp_getstring(env, "db.test.status", NULL);
	t( strcmp(v, "online") == 0 );
	free(v);

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 9 );
	t( sp_getint(o, "lsn") == 2ULL );
	t( sp_destroy(o) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
tpr_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "sophia.recover", 2) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_destroy(db) == 0 );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
tpr_test3(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "sophia.recover", 2) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.enable", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	t( sp_open(env) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
}

stgroup *tpr_group(void)
{
	stgroup *group = st_group("two_phase_recover");
	st_groupadd(group, st_test("recover_open", tpr_test0));
	st_groupadd(group, st_test("recover_reply", tpr_test1));
	st_groupadd(group, st_test("recover_shutdown0", tpr_test2));
	st_groupadd(group, st_test("recover_shutdown1", tpr_test3));
	return group;
}
