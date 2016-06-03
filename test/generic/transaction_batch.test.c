
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
transaction_batch_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *tx = sp_begin(env);
	t( tx != NULL );
	t( sp_setstring(tx, "isolation", "batch", 0) == 0 );

	t( sp_setstring(tx, "isolation", "batch", 0) == -1 );
	t( sp_setstring(tx, "isolation", "serializable", 0) == -1 );

	uint32_t a = 7;
	uint32_t b = 8;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &a, sizeof(a)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &b, sizeof(a)) == 0 );
	t( sp_set(tx, o) == 0 );

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &a, sizeof(a)) == 0 );
	o = sp_get(tx, o); /* error */

	t( sp_commit(tx) == 0 );

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &a, sizeof(a)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
transaction_batch_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *tx = sp_begin(env);
	t( tx != NULL );

	uint32_t a = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &a, sizeof(a)) == 0 );
	t( sp_set(tx, o) == 0 );

	t( sp_setstring(tx, "isolation", "batch", 0) == -1 );
	t( sp_destroy(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *transaction_batch_group(void)
{
	stgroup *group = st_group("transaction_batch");
	st_groupadd(group, st_test("test0", transaction_batch_test0));
	st_groupadd(group, st_test("test1", transaction_batch_test1));
	return group;
}
