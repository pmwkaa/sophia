
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
snapshot_create_delete(stc *cx ssunused)
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
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	t( sp_set(c, "snapshot", "test_snapshot") == 0 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );
	t( sp_drop(snapshot) == 0 );
	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot == NULL );

	t( sp_destroy(env) == 0 );
}

static void
snapshot_cursor(stc *cx ssunused)
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
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_set(c, "snapshot", "test_snapshot") == 0 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	i = 0;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	cur = sp_cursor(snapshot, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
snapshot_get(stc *cx ssunused)
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
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_set(c, "snapshot", "test_snapshot") == 0 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(snapshot, o);
		t( *(int*)sp_get(o, "value", NULL) == i );
		t( sp_destroy(o) == 0 );
		i++;
	}

	t( sp_destroy(env) == 0 );
}

static void
snapshot_recover_cursor(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_set(c, "snapshot", "test_snapshot") == 0 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );

	t( sp_set(c, "snapshot", "test_snapshot") == -1 );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *o = sp_get(c, "snapshot.test_snapshot.lsn");
	t( o != NULL );
	char *lsn = strdup(sp_get(o, "value", NULL));
	sp_destroy(o);

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );

	/* recover snapshot */
	t( sp_set(c, "snapshot", "test_snapshot") == 0 );
	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );
	t( sp_set(c, "snapshot.test_snapshot.lsn", lsn) == 0 );
	o = sp_get(c, "snapshot.test_snapshot.lsn");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), lsn) == 0 );
	sp_destroy(o);
	free(lsn);

	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	i = 0;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	i = 0;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	cur = sp_cursor(snapshot, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static void
snapshot_recover_get(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_set(c, "snapshot", "test_snapshot") == 0 );
	void *snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		int value = i + 1;
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *o = sp_get(c, "snapshot.test_snapshot.lsn");
	t( o != NULL );
	char *lsn = strdup(sp_get(o, "value", NULL));
	sp_destroy(o);

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );

	/* recover snapshot */
	t( sp_set(c, "snapshot", "test_snapshot") == 0 );
	snapshot = sp_get(c, "snapshot.test_snapshot");
	t( snapshot != NULL );
	t( sp_set(c, "snapshot.test_snapshot.lsn", lsn) == 0 );
	o = sp_get(c, "snapshot.test_snapshot.lsn");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), lsn) == 0 );
	sp_destroy(o);
	free(lsn);

	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(db, o);
		t( *(int*)sp_get(o, "value", NULL) == i + 1 );
		t( sp_destroy(o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(snapshot, o);
		t( *(int*)sp_get(o, "value", NULL) == i );
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
