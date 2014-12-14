
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
tpc_prepare_commit_empty(stc *cx)
{
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
tpc_prepare_rollback_empty(stc *cx)
{
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
tpc_prepare_prepare_empty(stc *cx)
{
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
tpc_prepare_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
tpc_prepare_rollback(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
tpc_prepare_prepare(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_prepare(tx);
	t( rc == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
tpc_prepare_set(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(cx->env);
	t( a != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_prepare(a);
	t( rc == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == -1 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
}

static void
tpc_prepare_wait0(stc *cx)
{
	void *db = cx->db;
	int rc;

	void *a = sp_begin(cx->env);
	t( a != NULL );
	void *b = sp_begin(cx->env);
	t( b != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	t( sp_commit(b) == 2 ); /* wait */
	st_transaction(cx);

	rc = sp_prepare(a);
	t( rc == 0 );
	st_transaction(cx);

	t( sp_commit(b) == 2 ); /* wait */
	st_transaction(cx);

	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	t( sp_commit(b) == 1 ); /* rlb */
	st_transaction(cx);
}

static void
tpc_prepare_wait1(stc *cx)
{
	void *db = cx->db;
	int rc;

	void *a = sp_begin(cx->env);
	t( a != NULL );
	void *b = sp_begin(cx->env);
	t( b != NULL );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(a, o) == 0 );

	rc = sp_prepare(a);
	t( rc == 2 ); /* wait */
	st_transaction(cx);

	t( sp_commit(b) == 0 ); /* commit */
	st_transaction(cx);

	t( sp_prepare(a) == 1 ); /* rlb */
	st_transaction(cx);
}

stgroup *tpc_group(void)
{
	stgroup *group = st_group("two_phase_commit");
	st_groupadd(group, st_test("prepare_commit_empty", tpc_prepare_commit_empty));
	st_groupadd(group, st_test("prepare_rollback_empty", tpc_prepare_rollback_empty));
	st_groupadd(group, st_test("prepare_prepare_empty", tpc_prepare_prepare_empty));
	st_groupadd(group, st_test("prepare_commit", tpc_prepare_commit));
	st_groupadd(group, st_test("prepare_rollback", tpc_prepare_rollback));
	st_groupadd(group, st_test("prepare_prepare", tpc_prepare_prepare));
	st_groupadd(group, st_test("prepare_set", tpc_prepare_set));
	st_groupadd(group, st_test("prepare_wait0", tpc_prepare_wait0));
	st_groupadd(group, st_test("prepare_wait1", tpc_prepare_wait1));
	return group;
}
