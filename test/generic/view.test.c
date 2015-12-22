
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
view_create_delete(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	void *view = sp_getobject(env, "view.test_view");
	t( view != NULL );
	t( sp_destroy(view) == 0 );
	view = sp_getobject(env, "view.test_view");
	t( view == NULL );

	t( sp_destroy(env) == 0 );
}

static void
view_cursor(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	void *view = sp_getobject(env, "view.test_view");
	t( view != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	i = 0;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	cur = sp_cursor(view);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
view_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	void *view = sp_getobject(env, "view.test_view");
	t( view != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(view, o);
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		t( sp_destroy(o) == 0 );
		i++;
	}

	t( sp_destroy(env) == 0 );
}

static void
view_recover_cursor(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	void *view = sp_getobject(env, "view.test_view");
	t( view != NULL );

	t( sp_setstring(env, "view", "test_view", 0) == -1 );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	int64_t lsn = sp_getint(env, "view.test_view.lsn");

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );

	/* recover view */
	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	view = sp_getobject(env, "view.test_view");
	t( view != NULL );
	t( sp_setint(env, "view.test_view.lsn", lsn) == 0 );
	t( sp_getint(env, "view.test_view.lsn") == lsn );

	t( sp_open(env) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );

	i = 0;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	i = 0;
	o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	cur = sp_cursor(view);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
view_recover_get(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	void *view = sp_getobject(env, "view.test_view");
	t( view != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	int64_t lsn = sp_getint(env, "view.test_view.lsn");

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );

	/* recover view */
	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	view = sp_getobject(env, "view.test_view");
	t( view != NULL );
	t( sp_setint(env, "view.test_view.lsn", lsn) == 0 );

	t( sp_open(env) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(db, o);
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1 );
		t( sp_destroy(o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(view, o);
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		t( sp_destroy(o) == 0 );
		i++;
	}

	t( sp_destroy(env) == 0 );
}

static void
view_db_view_only(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while ( i < 10 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "view", "test_view", 0) == 0 );
	void *view = sp_getobject(env, "view.test_view");
	t( view != NULL );

	t( sp_setint(view, "db-view-only", 1) == 0 );
	t( sp_setint(view, "db-view-only", 1) == -1 );

	void *o = sp_document(db);
	i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(view, o);
	t( o == NULL );

	t( sp_cursor(view) == NULL );

	t( sp_destroy(env) == 0 );
}

stgroup *view_group(void)
{
	stgroup *group = st_group("view");
	st_groupadd(group, st_test("create_delete", view_create_delete));
	st_groupadd(group, st_test("cursor", view_cursor));
	st_groupadd(group, st_test("get", view_get));
	st_groupadd(group, st_test("recover_cursor", view_recover_cursor));
	st_groupadd(group, st_test("recover_get", view_recover_get));
	st_groupadd(group, st_test("db_view_only", view_db_view_only));
	return group;
}
