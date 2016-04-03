
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
hc_prepare_commit(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );

	int key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);

	t( sp_setint(tx, "half_commit", 1) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
hc_prepare_commit_empty(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );
	t( sp_setint(tx, "half_commit", 1) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
hc_prepare_rollback0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );

	int key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);

	t( sp_setint(tx, "half_commit", 1) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
hc_prepare_rollback1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int rc;
	void *tx = sp_begin(env);
	t( tx != NULL );

	int key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);

	t( sp_setint(tx, "half_commit", 1) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_setint(tx, "half_commit", 1) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
hc_prepare_commit_conflict(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	int key = 7;

	int rc;
	void *a = sp_begin(env);
	t( a != NULL );

	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );
	t( sp_setint(a, "half_commit", 1) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );

	void *b = sp_begin(env);
	t( b != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );
	t( sp_setint(b, "half_commit", 1) == 0 );
	rc = sp_commit(b); /* this should fail in default conditions */
	t( rc == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *half_commit_group(void)
{
	stgroup *group = st_group("half_commit");
	st_groupadd(group, st_test("prepare_commit_empty", hc_prepare_commit_empty));
	st_groupadd(group, st_test("prepare_commit", hc_prepare_commit));
	st_groupadd(group, st_test("prepare_rollback0", hc_prepare_rollback0));
	st_groupadd(group, st_test("prepare_rollback1", hc_prepare_rollback1));
	st_groupadd(group, st_test("prepare_commit_conflict", hc_prepare_commit_conflict));
	return group;
}
