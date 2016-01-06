
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
cache_validate(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.cache_mode", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == -1 );

	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(db, o) == -1 );

	t( sp_destroy(env) == 0 );
}

static void
cache_assign0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
cache_assign1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "string",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == -1 );
	t( sp_destroy(env) == 0 );
}

static void
cache_set(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 0 ); /* washed out */

	t( sp_destroy(env) == 0 );
}

static void
cache_set_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 ); /* set */
	t( sp_getint(o, "lsn") == 1 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 2 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_getint(env, "db.cache.index.count") == 1 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	/* ensure we read from cache next time */
	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == SVGET );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
cache_set_get_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == SVGET );
	t( sp_getint(o, "lsn") == 2 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	/* ensure we read from cache next time */
	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == SVGET );
	t( sp_getint(o, "lsn") == 3 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
cache_set_get_set(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 2 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 2 );
	t( sp_getint(env, "db.cache.index.count") == 3 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 0 );

	/* ensure we read from Storage next time */
	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 );
	t( sp_getint(o, "lsn") == 3 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
cache_set_get_set_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 2 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 2 );
	t( sp_getint(env, "db.cache.index.count") == 3 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 0 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	/* ensure we read from Cache this time */
	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == SVGET );
	t( sp_getint(o, "lsn") == 4 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
cache_set_get_set_set(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 ); /* set */
	t( sp_getint(o, "lsn") == 1 );
	t( sp_destroy(o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 124;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 125;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 3 );
	t( sp_getint(env, "db.cache.index.count") == 4 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );

	t( sp_getint(env, "db.cache.index.count") == 1 ); /* washout replaces */

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 3 );

	t( sp_destroy(env) == 0 );
}

static void
cache_set_get_set_set_amqf(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );
	t( sp_setint(env, "db.cache.amqf", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 0 ); /* skipped by amqf */

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 ); /* set */
	t( sp_getint(o, "lsn") == 1 );
	t( sp_destroy(o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 124;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 125;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 3 );
	t( sp_getint(env, "db.cache.index.count") == 1 ); /* only get + 3 sets skipped by amqf */

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );

	t( sp_getint(env, "db.cache.index.count") == 1 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 3 );

	t( sp_destroy(env) == 0 );
}

static void
cache_delete(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 0 );
	t( sp_getint(env, "db.cache.index.count") == 0 );

	t( sp_destroy(env) == 0 );
}

static void
cache_set_get_delete(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );

	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 ); /* set */
	t( sp_getint(o, "lsn") == 1 );
	t( sp_destroy(o) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 2 );
	t( sp_getint(env, "db.cache.index.count") == 3 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_getint(env, "db.cache.index.count") == 1 ); /* delete left */
	t( sp_setint(env, "db.cache.compact", 0) == 0 );
	t( sp_getint(env, "db.cache.index.count") == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
cache_recover0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 ); /* set */
	t( sp_getint(o, "lsn") == 1 );
	t( sp_destroy(o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 124;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 125;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 3 );
	t( sp_getint(env, "db.cache.index.count") == 4 );

	sp_destroy(env);

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );
	db = sp_getobject(env, "db.test");
	t(db != NULL);

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );

	t( sp_getint(env, "db.cache.index.count") == 0 ); /* get not put in log */

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( sp_getint(env, "db.test.index.count") == 3 );

	t( sp_destroy(env) == 0 );
}

static void
cache_recover1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t(db != NULL);

	void *tx = sp_begin(env);
	t( tx != NULL );
	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 123;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 1 );
	t( sp_getint(env, "db.cache.index.count") == 1 );

	tx = sp_begin(env);
	t( tx != NULL );
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( sp_getint(o, "flags") == 0 ); /* set */
	t( sp_getint(o, "lsn") == 1 );
	t( sp_destroy(o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 124;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_document(db);
	t( o != NULL );
	key = 125;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_getint(env, "db.test.index.count") == 3 );
	t( sp_getint(env, "db.cache.index.count") == 4 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );

	t( sp_getint(env, "db.cache.index.count") == 1 );

	sp_destroy(env);

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "db", "cache", 0) == 0 );
	t( sp_setstring(env, "db.cache.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.cache.sync", 0) == 0 );
	t( sp_setint(env, "db.cache.cache_mode", 1) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32",0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setstring(env, "db.test.cache", "cache", 0) == 0 );
	t( sp_open(env) == 0 );
	db = sp_getobject(env, "db.test");
	t(db != NULL);

	t( sp_getint(env, "db.cache.index.count") == 3 ); /* after reply */
	t( sp_getint(env, "db.test.index.count") == 3 );

	t( sp_setint(env, "db.cache.branch", 0) == 0 );
	t( sp_setint(env, "db.cache.compact", 0) == 0 );

	t( sp_getint(env, "db.cache.index.count") == 1 );
	t( sp_getint(env, "db.test.index.count") == 3 );

	t( sp_destroy(env) == 0 );
}

stgroup *cache_group(void)
{
	stgroup *group = st_group("cache");
	st_groupadd(group, st_test("validate", cache_validate));
	st_groupadd(group, st_test("assign0", cache_assign0));
	st_groupadd(group, st_test("assign1", cache_assign1));
	st_groupadd(group, st_test("set", cache_set));
	st_groupadd(group, st_test("set_get", cache_set_get));
	st_groupadd(group, st_test("set_get_get", cache_set_get_get));
	st_groupadd(group, st_test("set_get_set", cache_set_get_set));
	st_groupadd(group, st_test("set_get_set_get", cache_set_get_set_get));
	st_groupadd(group, st_test("set_get_set_set", cache_set_get_set_set));
	st_groupadd(group, st_test("set_get_set_set_amqf", cache_set_get_set_set_amqf));
	st_groupadd(group, st_test("delete", cache_delete));
	st_groupadd(group, st_test("set_get_delete", cache_set_get_delete));
	st_groupadd(group, st_test("recover0", cache_recover0));
	st_groupadd(group, st_test("recover1", cache_recover1));
	return group;
}
