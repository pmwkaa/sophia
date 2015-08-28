
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
snapshot_create_delete(void)
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

	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == 0 );
	void *snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot != NULL );
	t( sp_destroy(snapshot) == 0 );
	snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_cursor(void)
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
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == 0 );
	void *snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	void *o = sp_object(db);
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
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	cur = sp_cursor(snapshot);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
snapshot_get(void)
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
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == 0 );
	void *snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(snapshot, o);
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		t( sp_destroy(o) == 0 );
		i++;
	}

	t( sp_destroy(env) == 0 );
}

static void
snapshot_recover_cursor(void)
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
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == 0 );
	void *snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot != NULL );

	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == -1 );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	int64_t lsn = sp_getint(env, "snapshot.test_snapshot.lsn");

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

	/* recover snapshot */
	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == 0 );
	snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot != NULL );
	t( sp_setint(env, "snapshot.test_snapshot.lsn", lsn) == 0 );
	t( sp_getint(env, "snapshot.test_snapshot.lsn") == lsn );

	t( sp_open(env) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );

	i = 0;
	void *o = sp_object(db);
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
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	cur = sp_cursor(snapshot);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
snapshot_recover_get(void)
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
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == 0 );
	void *snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	int64_t lsn = sp_getint(env, "snapshot.test_snapshot.lsn");

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

	/* recover snapshot */
	t( sp_setstring(env, "snapshot", "test_snapshot", 0) == 0 );
	snapshot = sp_getobject(env, "snapshot.test_snapshot");
	t( snapshot != NULL );
	t( sp_setint(env, "snapshot.test_snapshot.lsn", lsn) == 0 );

	t( sp_open(env) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(db, o);
		t( *(int*)sp_getstring(o, "value", NULL) == i + 1 );
		t( sp_destroy(o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(snapshot, o);
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		t( sp_destroy(o) == 0 );
		i++;
	}

	t( sp_destroy(env) == 0 );
}

stgroup *snapshot_group(void)
{
	stgroup *group = st_group("snapshot");
	st_groupadd(group, st_test("create_delete", snapshot_create_delete));
	st_groupadd(group, st_test("cursor", snapshot_cursor));
	st_groupadd(group, st_test("get", snapshot_get));
	st_groupadd(group, st_test("recover_cursor", snapshot_recover_cursor));
	st_groupadd(group, st_test("recover_get", snapshot_recover_get));
	return group;
}
