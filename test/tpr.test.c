
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
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "log.two_phase_recover", "1") == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "recover") == 0 );
	sp_destroy(o);

	t( sp_open(env) == 0 ); /* complete */

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
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "log.two_phase_recover", "1") == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	t( sp_open(env) == 0 );
	int key = 7;
	int value = 8;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(c, "db.test.branch") == 0 );
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
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "log.two_phase_recover", "1") == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32", NULL) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	o = sp_get(c, "db.test.status");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "recover") == 0 );
	sp_destroy(o);

	void *tx = sp_begin(env);
	t( tx != NULL );
	key = 7;
	value = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_prepare(tx, 1ULL) == 0 );
	t( sp_commit(tx) == 0 ); /* skip */

	tx = sp_begin(env);
	t( tx != NULL );
	key = 7;
	value = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_prepare(tx, 2ULL) == 0 );
	t( sp_commit(tx) == 0 ); /* commit */

	t( sp_open(env) == 0 );

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
