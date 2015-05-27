
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
gc_test0(stc *cx ssunused)
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
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *t0 = sp_begin(env);

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.branch") == 0 );

	i = 0;
	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "200") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	t( sp_set(c, "db.test.compact") == 0 );

	i = 0;
	o = sp_object(db);
	t( o != NULL );
	cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "200") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	sp_destroy(t0);
	t( sp_destroy(env) == 0 );
}

static void
gc_test1(stc *cx ssunused)
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
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *t0 = sp_begin(env);

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.branch") == 0 );

	i = 0;
	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "200") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	t( sp_set(c, "db.test.compact") == 0 );

	i = 0;
	o = sp_object(db);
	t( o != NULL );
	cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "200") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	sp_destroy(t0);
	t( sp_destroy(env) == 0 );
}

static void
gc_test2(stc *cx ssunused)
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
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *t0 = sp_begin(env);
	t( sp_set(c, "db.test.branch") == 0 );

	i = 0;
	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	t( sp_set(c, "db.test.compact") == 0 );

	i = 0;
	o = sp_object(db);
	t( o != NULL );
	cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	sp_destroy(t0);
	t( sp_destroy(env) == 0 );
}

static void
gc_test3(stc *cx ssunused)
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
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *t0 = sp_begin(env);

	int value = 0;
	int i = 0;
	while ( i < 100 ) {
		value = i;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	i = 0;
	while ( i < 100 ) {
		value = i + 1;
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	i = 0;
	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	while (sp_get(cur)) {
		o = sp_object(cur);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i + 1);
		i++;
	}
	sp_destroy(cur);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "200") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	sp_destroy(t0);

	/* gc */
	o = sp_get(c, "scheduler.gc_active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	t( sp_set(c, "scheduler.gc") == 0 );

	o = sp_get(c, "scheduler.gc_active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	t( sp_set(c, "scheduler.run") == 1 );
	t( sp_set(c, "scheduler.run") == 0 ); /* execute */

	o = sp_get(c, "scheduler.gc_active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.count_dup");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

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
