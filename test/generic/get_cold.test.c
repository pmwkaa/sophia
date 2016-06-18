
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
get_cold_test0(void)
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
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	uint32_t key = 7;
	uint32_t value = 1;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.compaction.branch", 0) == 0 );

	value = 2;
	key = 7;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.compaction.branch", 0) == 0 );

	/* default */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 2 );
	sp_destroy(o);

	/* cold only */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setint(o, "cold_only", 1) == 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

static void
get_cold_test1(void)
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
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	uint32_t key = 7;
	uint32_t value = 1;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.compaction.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	value = 2;
	key = 7;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.compaction.branch", 0) == 0 );

	/* default */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 2 );
	sp_destroy(o);

	/* cold only */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setint(o, "cold_only", 1) == 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 1 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
get_cold_test2(void)
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
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	uint32_t key = 7;
	uint32_t value = 1;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.compaction.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compaction.compact", 0) == 0 );

	value = 2;
	key = 7;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.compaction.branch", 0) == 0 );

	/* default */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 2 );

	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 2 );
	o = sp_get(db, o);
	t( o == NULL );

	/* cold only */
	o = sp_document(db);
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	t( sp_setint(o, "cold_only", 1) == 0 );
	t( o != NULL );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 1 );

	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 1 );
	o = sp_get(db, o);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

stgroup *get_cold_group(void)
{
	stgroup *group = st_group("get_cold");
	st_groupadd(group, st_test("test0", get_cold_test0));
	st_groupadd(group, st_test("test1", get_cold_test1));
	st_groupadd(group, st_test("test2", get_cold_test2));
	return group;
}
