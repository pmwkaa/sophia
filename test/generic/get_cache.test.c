
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
get_cache_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	uint32_t key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	/* default */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	/* cache only */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setint(o, "cache_only", 1) == 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	/* default */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	/* cache only */
	o = sp_document(db);
	t( o != NULL );
	t( sp_setint(o, "cache_only", 1) == 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

static void
get_cache_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");

	uint32_t key = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	key = 8;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	void *cursor;

	/* default */
	cursor = sp_cursor(env);
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(cursor, o);
	t( o != NULL );
	sp_destroy(o);
	sp_destroy(cursor);

	/* cache only */
	cursor = sp_cursor(env);
	o = sp_document(db);
	t( o != NULL );
	t( sp_setint(o, "cache_only", 1) == 0 );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(cursor, o);
	t( o == NULL );
	sp_destroy(cursor);

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	/* start by filling cache (gte) */
	cursor = sp_cursor(env);
	o = sp_document(db);
	t( o != NULL );
	o = sp_get(cursor, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "key", NULL) == 7 );

	/* continue from cache */
	t( sp_setint(o, "cache_only", 1) == 0 );

	o = sp_get(cursor, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "key", NULL) == 8 );
	sp_destroy(o);
	sp_destroy(cursor);

	t( sp_destroy(env) == 0 );
}

stgroup *get_cache_group(void)
{
	stgroup *group = st_group("get_cache");
	st_groupadd(group, st_test("test0", get_cache_test0));
	st_groupadd(group, st_test("test1", get_cache_test1));
	return group;
}
