
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
prefix_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	char key[] = "a";
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyb[] = "ab";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keyb, sizeof(keyb)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyc[] = "aba";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keyc, sizeof(keyc)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyd[] = "abac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keyd, sizeof(keyd)) == 0 );
	t( sp_set(db, o) == 0 );

	char keye[] = "bbac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keye, sizeof(keye)) == 0 );
	t( sp_set(db, o) == 0 );

	char prefix[] = "ab";

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	t( sp_setstring(o, "prefix", prefix, sizeof(prefix) - 1) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	o = sp_get(c, NULL);
	t( o != NULL );
	t( strcmp((char*)sp_getstring(o, "key", NULL), "ab") == 0 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	t( strcmp((char*)sp_getstring(o, "key", NULL), "aba") == 0 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	t( strcmp((char*)sp_getstring(o, "key", NULL), "abac") == 0 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

static void
prefix_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	char key[] = "a";
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyb[] = "ab";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keyb, sizeof(keyb)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyc[] = "aba";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keyc, sizeof(keyc)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyd[] = "abac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keyd, sizeof(keyd)) == 0 );
	t( sp_set(db, o) == 0 );

	char keye[] = "bbac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "key", keye, sizeof(keye)) == 0 );
	t( sp_set(db, o) == 0 );

	char prefix[] = "ab";

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", "<=", 0) == 0 );
	t( sp_setstring(o, "prefix", prefix, sizeof(prefix)) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( strcmp((char*)sp_getstring(o, "key", NULL), "ab") == 0 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

stgroup *prefix_group(void)
{
	stgroup *group = st_group("prefix");
	st_groupadd(group, st_test("test0", prefix_test0));
	st_groupadd(group, st_test("test1", prefix_test1));
	return group;
}
