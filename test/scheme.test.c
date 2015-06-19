
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libss.h>
#include <libst.h>
#include <sophia.h>

static void
schema_test0(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.compression", "zstd") == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "string") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_get(c, "db.test.compression");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "zstd") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.key");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "u32") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.key_b");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "string") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
schema_test1(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.compression_key", "1") == 0 );
	t( sp_set(c, "db.test.compression", "none") == 0 );
	t( sp_set(c, "db.test.index.key", "u32") == 0 );
	t( sp_set(c, "db.test.index.key", "u32") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "string") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.compression", "zstd") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.key", "string", NULL) == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "u64") == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_get(c, "db.test.compression");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "none") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.compression_key");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.key");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "u32") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.key");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "u32") == 0 );
	sp_destroy(o);

	o = sp_get(c, "db.test.index.key_b");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "string") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *schema_group(void)
{
	stgroup *group = st_group("schema");
	st_groupadd(group, st_test("test0", schema_test0));
	st_groupadd(group, st_test("test1", schema_test1));
	return group;
}
