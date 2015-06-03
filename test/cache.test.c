
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libst.h>
#include <sophia.h>

static void
cache_test0(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 185) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.branch") == 0 );

	void *o = sp_object(db);
	void *cur = sp_cursor(db, o);
	i = 0;
	t( cur != NULL );
	while ((o = sp_get(cur))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
		sp_destroy(o);
	}
	t( i == 185 );
	t( sp_destroy(cur) == 0 );

	o = sp_get(c, "db.test.index.read_disk");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.read_cache");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "184") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
cache_test1(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 185) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.branch") == 0 );
	i = 185;
	while (i < 370) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( i == 370 );
	t( sp_set(c, "db.test.branch") == 0 );

	void *o = sp_object(db);
	void *cur = sp_cursor(db, o);
	i = 0;
	t( cur != NULL );
	while ((o = sp_get(cur))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
		sp_destroy(o);
	}
	t( sp_destroy(cur) == 0 );

	o = sp_get(c, "db.test.index.read_disk");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "2") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.read_cache");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "553") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
cache_invalidate(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 185) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.branch") == 0 );

	i = 185;
	while (i < 370) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( i == 370 );
	t( sp_set(c, "db.test.branch") == 0 );

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	void *cur = sp_cursor(db, o);
	i = 0;
	t( cur != NULL );
	while ((o = sp_get(cur))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		if (i == 200) {
			t( sp_set(c, "db.test.branch") == 0 );
		}
		i++;
		sp_destroy(o);
	}
	t( i == 371 );
	t( sp_destroy(cur) == 0 );

	o = sp_get(c, "db.test.index.read_disk");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "3") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.read_cache");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "722") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *cache_group(void)
{
	stgroup *group = st_group("cache");
	st_groupadd(group, st_test("single_branch", cache_test0));
	st_groupadd(group, st_test("double_branch", cache_test1));
	st_groupadd(group, st_test("invalidate", cache_invalidate));
	return group;
}
