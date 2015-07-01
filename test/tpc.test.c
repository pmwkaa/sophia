
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
tpc_prepare_commit_empty(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	void *tx = sp_begin(env);
	t( tx != NULL );
	int rc;
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_rollback_empty(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_prepare_empty(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_destroy(tx);

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_commit(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_rollback(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_prepare(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_set(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *a = sp_begin(env);
	t( a != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_prepare(a);
	t( rc == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == -1 );
	rc = sp_commit(a);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_wait0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *a = sp_begin(env);
	t( a != NULL );
	void *b = sp_begin(env);
	t( b != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_commit(b) == 2 ); /* wait */

	rc = sp_prepare(a);
	t( rc == 0 );

	t( sp_commit(b) == 2 ); /* wait */

	rc = sp_commit(a);
	t( rc == 0 );

	t( sp_commit(b) == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
tpc_prepare_wait1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *a = sp_begin(env);
	t( a != NULL );
	void *b = sp_begin(env);
	t( b != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );

	rc = sp_prepare(a);
	t( rc == 2 ); /* wait */

	t( sp_commit(b) == 0 ); /* commit */
	t( sp_prepare(a) == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

stgroup *tpc_group(void)
{
	stgroup *group = st_group("two_phase_commit");
	st_groupadd(group, st_test("prepare_commit_empty", tpc_prepare_commit_empty));
	st_groupadd(group, st_test("prepare_rollback_empty", tpc_prepare_rollback_empty));
	st_groupadd(group, st_test("prepare_prepare_empty", tpc_prepare_prepare_empty));
	st_groupadd(group, st_test("prepare_commit", tpc_prepare_commit));
	st_groupadd(group, st_test("prepare_rollback", tpc_prepare_rollback));
	st_groupadd(group, st_test("prepare_prepare", tpc_prepare_prepare));
	st_groupadd(group, st_test("prepare_set", tpc_prepare_set));
	st_groupadd(group, st_test("prepare_wait0", tpc_prepare_wait0));
	st_groupadd(group, st_test("prepare_wait1", tpc_prepare_wait1));
	return group;
}
