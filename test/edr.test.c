
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
edr_test0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", NULL) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.edr", "1") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "recover") == 0 );
	sp_destroy(o);

	t( sp_open(db) == 0 );

	o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "online") == 0 );
	sp_destroy(o);

	t( sp_open(db) == -1 );

	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
}

#if 0
static void
edr_test1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", NULL) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.edr", "1") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	t( sp_open(db) == 0 );
	int key = 7;
	int value = 8;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(sp_ctl(c), "db.test.run_branch") == 0 );
	key = 7;
	value = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", NULL) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.edr", "1") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "recover") == 0 );
	sp_destroy(o);

	/*
	int value = 8;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(sp_ctl(c), "db.test.run_branch") == 0 );
	*/
	/*
	key = 7;
	value = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_destroy(env) == 0 );
	*/

	t( sp_open(db) == 0 );


	void *o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "recover") == 0 );
	sp_destroy(o);

	t( sp_open(db) == 0 );

	o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "online") == 0 );
	sp_destroy(o);

	t( sp_open(db) == -1 );

	t( sp_destroy(env) == 0 );
}
#endif

stgroup *edr_group(void)
{
	stgroup *group = st_group("edr");
	st_groupadd(group, st_test("recover_open", edr_test0));
	/*st_groupadd(group, st_test("recover_reply", edr_test1));*/
	return group;
}
