
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
transaction_md_set_commit(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "t0", 0) == 0 );
	t( sp_setstring(env, "db", "t1", 0) == 0 );
	t( sp_setstring(env, "db.t0.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.t1.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.t0.index.key", "u32", 0) == 0 );
	t( sp_setstring(env, "db.t1.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.t0.sync", 0) == 0 );
	t( sp_setint(env, "db.t1.sync", 0) == 0 );

	void *t0 = sp_getobject(env, "db.t0");
	t( t0 != NULL );
	void *t1 = sp_getobject(env, "db.t1");
	t( t1 != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 7;
	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(t0);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(t1);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	o = sp_document(t0);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(t0, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);

	o = sp_document(t1);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(t1, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
transaction_md_set_rollback(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "t0", 0) == 0 );
	t( sp_setstring(env, "db", "t1", 0) == 0 );
	t( sp_setstring(env, "db.t0.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.t1.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.t0.index.key", "u32", 0) == 0 );
	t( sp_setstring(env, "db.t1.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.t0.sync", 0) == 0 );
	t( sp_setint(env, "db.t1.sync", 0) == 0 );

	void *t0 = sp_getobject(env, "db.t0");
	t( t0 != NULL );
	void *t1 = sp_getobject(env, "db.t1");
	t( t1 != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 7;
	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(t0);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(t1);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_destroy(tx) == 0 );

	o = sp_document(t0);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(t0, o);
	t( o == NULL );

	o = sp_document(t1);
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(t1, o);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

stgroup *transaction_md_group(void)
{
	stgroup *group = st_group("transaction_md");
	st_groupadd(group, st_test("set_commit_get", transaction_md_set_commit));
	st_groupadd(group, st_test("set_rollback_get", transaction_md_set_rollback));
	return group;
}
