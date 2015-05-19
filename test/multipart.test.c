
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>
#include <sophia.h>

static void
multipart_cmp_eq_key(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );

	void *o = sp_get(c, "db.test.index.key");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "u32") == 0 );
	sp_destroy(o);

	t( sp_open(env) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
multipart_schema(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "string") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );

	void *o = sp_get(c, "db.test.index.key");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "string") == 0 );
	sp_destroy(o);

	o = sp_get(c, "db.test.index.key_b");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "string") == 0 );
	sp_destroy(o);

	t( sp_set(c, "db.test.index.key_b", "u32") == 0 );

	o = sp_get(c, "db.test.index.key_b");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "u32") == 0 );
	sp_destroy(o);

	t( sp_open(env) == 0 );
	t( sp_set(c, "db.test.index.key_b", "string") == -1 );
	t( sp_destroy(env) == 0 );
}

static void
multipart_set_get0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "string") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "u32") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	char key_a[] = "hello";
	uint32_t key_b = 7;

	void *o = sp_object(db);
	sp_set(o, "key", key_a, sizeof(key_a));
	t( sp_set(db, o) == -1 );

	o = sp_object(db);
	sp_set(o, "key", key_a, sizeof(key_a));
	sp_set(o, "key_b", &key_b, sizeof(key_b));
	t( sp_set(db, o) == 0);

	o = sp_object(db);
	sp_set(o, "key", key_a, sizeof(key_a));
	o = sp_get(db, o);
	t( o == NULL );

	o = sp_object(db);
	sp_set(o, "key", key_a, sizeof(key_a));
	sp_set(o, "key_b", &key_b, sizeof(key_b));
	o = sp_get(db, o);
	t( o != NULL );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
multipart_set_get1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "string") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "u32") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	char key_a[] = "hello";
	uint32_t i = 0;
	while (i < 546) {
		void *o = sp_object(db);
		sp_set(o, "key", key_a, sizeof(key_a));
		sp_set(o, "key_b", &i, sizeof(i));
		sp_set(o, "value", &i, sizeof(i));
		t( sp_set(db, o) == 0);
		i++;
	}

	i = 0;
	while (i < 546) {
		void *o = sp_object(db);
		sp_set(o, "key", key_a, sizeof(key_a));
		sp_set(o, "key_b", &i, sizeof(i));
		o = sp_get(db, o);
		t( o != NULL );
		uint32_t asize;
		t( strcmp(key_a, sp_get(o, "key", &asize)) == 0 );
		uint32_t bsize;
		t( *(uint32_t*)sp_get(o, "key_b", &bsize) == i );
		uint32_t vsize;
		t( *(uint32_t*)sp_get(o, "value", &vsize) == i );
		t( asize == sizeof(key_a) );
		t( bsize == sizeof(i) );
		t( vsize == sizeof(i) );
		sp_destroy(o);
		i++;
	}

	t( sp_destroy(env) == 0 );
}

static void
multipart_cursor0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "string") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "u32") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	char key_a[] = "hello";
	uint32_t i = 0;
	while (i < 546) {
		void *o = sp_object(db);
		sp_set(o, "key", key_a, sizeof(key_a));
		sp_set(o, "key_b", &i, sizeof(i));
		sp_set(o, "value", &i, sizeof(i));
		t( sp_set(db, o) == 0);
		i++;
	}

	i = 0;
	void *o = sp_object(db);
	void *cur = sp_cursor(db, o);
	t( cur != NULL );
	while ((o = sp_get(cur))) {
		uint32_t asize;
		t( strcmp(key_a, sp_get(o, "key", &asize)) == 0 );
		uint32_t bsize;
		t( *(uint32_t*)sp_get(o, "key_b", &bsize) == i );
		uint32_t vsize;
		t( *(uint32_t*)sp_get(o, "value", &vsize) == i );
		t( asize == sizeof(key_a) );
		t( bsize == sizeof(i) );
		t( vsize == sizeof(i) );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
multipart_cursor1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "string") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "u32") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	char key_a[] = "hello";
	uint32_t i = 0;
	while (i < 546) {
		void *o = sp_object(db);
		sp_set(o, "key", key_a, sizeof(key_a));
		sp_set(o, "key_b", &i, sizeof(i));
		sp_set(o, "value", &i, sizeof(i));
		t( sp_set(db, o) == 0);
		i++;
	}

	i = 322;
	void *o = sp_object(db);
	sp_set(o, "key", key_a, sizeof(key_a));
	sp_set(o, "key_b", &i, sizeof(i));
	sp_set(o, "value", &i, sizeof(i));
	void *cur = sp_cursor(db, o);
	t( cur != NULL );
	while ((o = sp_get(cur))) {
		uint32_t asize;
		t( strcmp(key_a, sp_get(o, "key", &asize)) == 0 );
		uint32_t bsize;
		t( *(uint32_t*)sp_get(o, "key_b", &bsize) == i );
		uint32_t vsize;
		t( *(uint32_t*)sp_get(o, "value", &vsize) == i );
		t( asize == sizeof(key_a) );
		t( bsize == sizeof(i) );
		t( vsize == sizeof(i) );
		i++;
	}
	sp_destroy(cur);
	t( i == 546 );

	t( sp_destroy(env) == 0 );
}

stgroup *multipart_group(void)
{
	stgroup *group = st_group("multipart");
	st_groupadd(group, st_test("cmp_eq_key", multipart_cmp_eq_key));
	st_groupadd(group, st_test("schema", multipart_schema));
	st_groupadd(group, st_test("set_get0", multipart_set_get0));
	st_groupadd(group, st_test("set_get1", multipart_set_get1));
	st_groupadd(group, st_test("cursor0", multipart_cursor0));
	st_groupadd(group, st_test("cursor1", multipart_cursor1));
	return group;
}
