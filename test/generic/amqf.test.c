
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
amqf_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.amqf", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 200) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	i = 0;
	while (i < 400) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(db, o);
		if (o) {
			sp_destroy(o);
		}
		i++;
	}
	t( sp_getint(env, "db.test.index.read_disk") == 200 );

	t( sp_destroy(env) == 0 );
}

static void
amqf_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key",0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.amqf", 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 100) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	i = 300;
	while (i < 500) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	i = 0;
	while (i < 400) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(db, o);
		if (o) {
			sp_destroy(o);
		}
		i++;
	}
	t( sp_getint(env, "db.test.index.read_disk") == 400 );

	t( sp_destroy(env) == 0 );
}

static void
amqf_test2(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key",0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.amqf", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 100) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	i = 300;
	while (i < 500) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );

	i = 0;
	while (i < 400) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(db, o);
		if (o) {
			sp_destroy(o);
		}
		i++;
	}
	t( sp_getint(env, "db.test.index.read_disk") == 232 ); /* see amqf_test1 */

	t( sp_destroy(env) == 0 );
}

static void
amqf_test3(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key",0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.amqf", 1) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while (i < 100) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	i = 300;
	while (i < 500) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	i = 0;
	while (i < 400) {
		void *o = sp_document(db);
		t( o != NULL );
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		o = sp_get(db, o);
		if (o) {
			sp_destroy(o);
		}
		i++;
	}
	t( sp_getint(env, "db.test.index.read_disk") == 232 );

	t( sp_destroy(env) == 0 );
}

stgroup *amqf_group(void)
{
	stgroup *group = st_group("amqf");
	st_groupadd(group, st_test("test0", amqf_test0));
	st_groupadd(group, st_test("test1", amqf_test1));
	st_groupadd(group, st_test("test2", amqf_test2));
	st_groupadd(group, st_test("test3", amqf_test3));
	return group;
}
