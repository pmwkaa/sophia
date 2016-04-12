
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
cursor_cache_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)",0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 185) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	void *cur = sp_cursor(env);
	i = 0;
	t( cur != NULL );
	void *o = sp_document(db);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", 0) == i );
		i++;
	}
	t( i == 185 );
	t( sp_destroy(cur) == 0 );

	t( sp_getint(env, "db.test.index.read_disk") == 1 );
	t( sp_getint(env, "db.test.index.read_cache") == 184 );

	t( sp_destroy(env) == 0 );
}

static void
cursor_cache_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)",0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 185) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	i = 185;
	while (i < 370) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( i == 370 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	void *cur = sp_cursor(env);
	t( cur != NULL );
	void *o = sp_document(db);
	i = 0;
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(cur) == 0 );

	t( sp_getint(env, "db.test.index.read_disk") == 2 );
	t( sp_getint(env, "db.test.index.read_cache") == 553 );
	t( sp_destroy(env) == 0 );
}

static void
cursor_cache_invalidate(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)",0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 185) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	i = 185;
	while (i < 370) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( i == 370 );
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	void *cur = sp_cursor(env);
	t( cur != NULL );
	o = sp_document(db);
	i = 0;
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		if (i == 200) {
			t( sp_setint(env, "db.test.branch", 0) == 0 );
		}
		i++;
	}
	t( i == 371 );
	t( sp_destroy(cur) == 0 );

	t( sp_getint(env, "db.test.index.read_disk") == 3 );
	t( sp_getint(env, "db.test.index.read_cache") == 722 );

	t( sp_destroy(env) == 0 );
}

stgroup *cursor_cache_group(void)
{
	stgroup *group = st_group("cursor_cache");
	st_groupadd(group, st_test("single_branch", cursor_cache_test0));
	st_groupadd(group, st_test("double_branch", cursor_cache_test1));
	st_groupadd(group, st_test("invalidate", cursor_cache_invalidate));
	return group;
}
