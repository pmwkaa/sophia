
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
prefix_test0(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );

	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	char key[] = "a";
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyb[] = "ab";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keyb, sizeof(keyb)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyc[] = "aba";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keyc, sizeof(keyc)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyd[] = "abac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keyd, sizeof(keyd)) == 0 );
	t( sp_set(db, o) == 0 );

	char keye[] = "bbac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keye, sizeof(keye)) == 0 );
	t( sp_set(db, o) == 0 );

	char prefix[] = "ab";

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	t( sp_set(o, "prefix", prefix, sizeof(prefix) - 1) == 0 );
	c = sp_cursor(db, o);
	t( c != NULL );

	o = sp_get(c);
	t( o != NULL );
	t( strcmp((char*)sp_get(o, "key", NULL), "ab") == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( strcmp((char*)sp_get(o, "key", NULL), "aba") == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( strcmp((char*)sp_get(o, "key", NULL), "abac") == 0 );
	o = sp_get(c);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

static void
prefix_test1(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );

	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	char key[] = "a";
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyb[] = "ab";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keyb, sizeof(keyb)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyc[] = "aba";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keyc, sizeof(keyc)) == 0 );
	t( sp_set(db, o) == 0 );

	char keyd[] = "abac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keyd, sizeof(keyd)) == 0 );
	t( sp_set(db, o) == 0 );

	char keye[] = "bbac";
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", keye, sizeof(keye)) == 0 );
	t( sp_set(db, o) == 0 );

	char prefix[] = "ab";

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", "<=") == 0 );
	t( sp_set(o, "prefix", prefix, sizeof(prefix)) == 0 );
	c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( strcmp((char*)sp_get(o, "key", NULL), "ab") == 0 );
	o = sp_get(c);
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
