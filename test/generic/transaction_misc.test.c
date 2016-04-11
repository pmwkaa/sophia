
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
transaction_misc_set_commit_get0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 7;
	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	/* skip range scan for concurrent scheme */
	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
transaction_misc_set_commit_get1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 7;
	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
transaction_get0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_document(db);
	t( o != NULL );
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *transaction_misc_group(void)
{
	stgroup *group = st_group("transaction_misc");
	st_groupadd(group, st_test("set_commit_get0", transaction_misc_set_commit_get0));
	st_groupadd(group, st_test("set_commit_get1", transaction_misc_set_commit_get1));
	st_groupadd(group, st_test("get", transaction_get0));
	return group;
}
