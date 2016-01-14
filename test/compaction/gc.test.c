
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
gc_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *t0 = sp_begin(env);

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	i = 0;
	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	t( sp_getint(env, "db.test.index.count") == 200 );
	t( sp_getint(env, "db.test.index.count_dup") == 100 );

	t( sp_setint(env, "db.test.compact", 0) == 0 );

	i = 0;
	o = sp_document(db);
	t( o != NULL );
	cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	t( sp_getint(env, "db.test.index.count") == 200 );
	t( sp_getint(env, "db.test.index.count_dup") == 100 );

	sp_destroy(t0);
	t( sp_destroy(env) == 0 );
}

static void
gc_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *t0 = sp_begin(env);

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	i = 0;
	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	t( sp_getint(env, "db.test.index.count") == 200 );
	t( sp_getint(env, "db.test.index.count_dup") == 100 );

	t( sp_setint(env, "db.test.compact", 0) == 0 );

	i = 0;
	o = sp_document(db);
	t( o != NULL );
	cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	t( sp_getint(env, "db.test.index.count") == 200 );
	t( sp_getint(env, "db.test.index.count_dup") == 100 );

	sp_destroy(t0);
	t( sp_destroy(env) == 0 );
}

static void
gc_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *t0 = sp_begin(env);
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	i = 0;
	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	t( sp_getint(env, "db.test.index.count") == 100 );
	t( sp_getint(env, "db.test.index.count_dup") == 0 );

	t( sp_setint(env, "db.test.compact", 0) == 0 );

	i = 0;
	o = sp_document(db);
	t( o != NULL );
	cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	t( sp_getint(env, "db.test.index.count") == 100 );
	t( sp_getint(env, "db.test.index.count_dup") == 0 );

	sp_destroy(t0);
	t( sp_destroy(env) == 0 );
}

static void
gc_test3(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *t0 = sp_begin(env);

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	i = 0;
	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	t( sp_getint(env, "db.test.index.count") == 200 );
	t( sp_getint(env, "db.test.index.count_dup") == 100 );

	sp_destroy(t0);

	/* gc */
	t( sp_getint(env, "scheduler.gc_active") == 0 );
	t( sp_setint(env, "scheduler.gc", 0) == 0 );
	t( sp_getint(env, "scheduler.gc_active") == 1 );

	t( sp_service(env) == 1 );
	t( sp_service(env) == 0 ); /* execute */

	t( sp_getint(env, "scheduler.gc_active") == 0 );

	t( sp_getint(env, "db.test.index.count") == 100 );
	t( sp_getint(env, "db.test.index.count_dup") == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *gc_group(void)
{
	stgroup *group = st_group("gc");
	st_groupadd(group, st_test("test0", gc_test0));
	st_groupadd(group, st_test("test1", gc_test1));
	st_groupadd(group, st_test("test2", gc_test2));
	st_groupadd(group, st_test("test3", gc_test3));
	return group;
}
