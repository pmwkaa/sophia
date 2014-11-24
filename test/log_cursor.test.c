
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
logcursor_empty0(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	void *lc = sp_ctl(tx, "log_cursor");
	t( lc != NULL );
	t( sp_get(lc) == NULL );
	t( sp_destroy(lc) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
logcursor_empty1(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	void *lc = sp_ctl(tx, "log_cursor");
	t( lc != NULL );
	t( sp_get(lc) == NULL );
	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
logcursor_iterate(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );

	int key = 7;
	void *o = sp_object(tx);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(tx);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(tx, o) == 0 );

	void *lc = sp_ctl(tx, "log_cursor");
	t( lc != NULL );
	o = sp_get(lc);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	t( *(uint64_t*)sp_get(o, "lsn", NULL) == 0ULL ); /* not prepared */
	o = sp_get(lc);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(lc);
	t( o == NULL );
	t( sp_destroy(lc) == 0 );

	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
logcursor_iterate_prepare(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );

	int key = 7;
	void *o = sp_object(tx);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(tx);
	sp_set(o, "key", &key, sizeof(key));
	t( sp_set(tx, o) == 0 );
	t( sp_prepare(tx) == 0 );

	void *lc = sp_ctl(tx, "log_cursor");
	t( lc != NULL );
	o = sp_get(lc);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	t( *(uint64_t*)sp_get(o, "lsn", NULL) == 1ULL );
	o = sp_get(lc);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	t( *(uint64_t*)sp_get(o, "lsn", NULL) == 1ULL );
	o = sp_get(lc);
	t( o == NULL );
	t( sp_destroy(lc) == 0 );

	t( sp_commit(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *logcursor_group(void)
{
	stgroup *group = st_group("log_cursor");
	st_groupadd(group, st_test("empty0", logcursor_empty0));
	st_groupadd(group, st_test("empty1", logcursor_empty1));
	st_groupadd(group, st_test("iterate", logcursor_iterate));
	st_groupadd(group, st_test("iterate_prepare", logcursor_iterate_prepare));
	return group;
}
