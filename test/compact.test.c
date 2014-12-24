
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
compact_delete0(stc *cx srunused)
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
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 20) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}
	t( sp_set(c, "db.test.branch") == 0 );

	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	t( o != NULL );
	int i = 0;
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( o != NULL );
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( i == 0 );

	t( sp_destroy(env) == 0 );
}

static void
compact_delete1(stc *cx srunused)
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
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 20) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}
	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	t( o != NULL );
	int i = 0;
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( o != NULL );
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( i == 0 );

	t( sp_destroy(env) == 0 );
}

static void
compact_delete_cursor(stc *cx srunused)
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
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 25) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}
	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	key = 0;
	while (key < 20) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_delete(db, o) == 0 );
		key++;
	}
	t( sp_set(c, "db.test.branch") == 0 );

	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	t( o != NULL );
	int i = 0;
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( o != NULL );
		t( *(int*)sp_get(o, "key", NULL) == 20 + i );
		i++;
	}
	t( i == 5 );

	t( sp_destroy(env) == 0 );
}

stgroup *compact_group(void)
{
	stgroup *group = st_group("compact");
	st_groupadd(group, st_test("delete_compaction0", compact_delete0));
	st_groupadd(group, st_test("delete_compaction1", compact_delete1));
	st_groupadd(group, st_test("delete_cursor", compact_delete_cursor));
	return group;
}
