
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
tpr_test0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.log_dir", NULL) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.two_phase_recover", "1") == 0 );
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

static void
tpr_test1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_open(env) == 0 );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.log_dir", NULL) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.two_phase_recover", "1") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	t( sp_set(c, "db.test.node_branch_wm", "1") == 0 );
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
	t( sp_set(c, "db.test.run_branch") == 0 );
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
	t( sp_set(c, "db.test.log_dir", NULL) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.two_phase_recover", "1") == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "recover") == 0 );
	sp_destroy(o);

	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	value = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(o, "lsn", 1ULL) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 ); /* skip */

	tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	value = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(o, "lsn", 2ULL) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 ); /* commit */

	t( sp_open(db) == 0 );

	o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "online") == 0 );
	sp_destroy(o);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 9 );
	t( *(uint64_t*)sp_get(o, "lsn", NULL) == 2ULL );
	t( sp_destroy(o) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *tpr_group(void)
{
	stgroup *group = st_group("two_phase_recover");
	st_groupadd(group, st_test("recover_open", tpr_test0));
	st_groupadd(group, st_test("recover_reply", tpr_test1));
	return group;
}
