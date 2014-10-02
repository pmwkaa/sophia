
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
transaction_rollback(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	st_transaction(cx);
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	st_transaction(cx);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_get_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
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
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_commit_get0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_commit_get1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 0;
	while (key < 10) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(tx, o) == 0 );
		key++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 0;
	tx = sp_begin(db);
	while (key < 10) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		o = sp_get(tx, o);
		t( o != NULL );
		t( *(int*)sp_get(o, "value", NULL) == key );
		sp_destroy(o);
		key++;
	}
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_rollback(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_rollback_get0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_rollback_get1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 0;
	while (key < 10) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(tx, o) == 0 );
		key++;
	}
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
	tx = sp_begin(db);
	key = 0;
	while (key < 10) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		o = sp_get(tx, o);
		t( o == NULL );
		key++;
	}
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_set_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	value = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_set_get_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	value = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_set_commit_get(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	value = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_set_rollback_get(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	value = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_delete_get_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(tx, o) == 0 );
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_set_delete_get_commit_get(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(tx, o) == 0 );
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o == NULL );
	st_transaction(cx);
}

static void
transaction_set_delete_set_commit_get(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(tx, o) == 0 );

	value = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value );
	sp_destroy(o);
	st_transaction(cx);
}

static void
transaction_set_delete_commit_get_set(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o == NULL );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value );
	sp_destroy(o);
	st_transaction(cx);
}

static void
transaction_p_set_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key_a = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_p_set_get_commit(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value_a = 10;
	int key_a = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	o = sp_get(a, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_a );
	sp_destroy(o);

	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	int value_b = 15;
	int key_b = 8;

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	o = sp_get(b, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_b );
	sp_destroy(o);

	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_p_set_commit_get0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value_a = 10;
	int key_a = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(a, o) == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_a );
	sp_destroy(o);
	o = sp_object(db);
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_b );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);
}

static void
transaction_p_set_commit_get1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );

	int value_a = 10;
	int key_a = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_a );
	sp_destroy(o);
	o = sp_object(db);
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_b );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);
}

static void
transaction_p_set_commit_get2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );

	int value_b = 15;
	int key_b = 8;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	int value_a = 10;
	int key_a = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_a );
	sp_destroy(o);
	o = sp_object(db);
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value_b );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);
}

static void
transaction_p_set_rollback_get0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value_a = 10;
	int key_a = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	o = sp_object(db);
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);
}

static void
transaction_p_set_rollback_get1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value_a = 10;
	int key_a = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	o = sp_object(db);
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);
}

static void
transaction_p_set_rollback_get2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );

	int value_b = 15;
	int key_b = 8;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);

	int value_a = 10;
	int key_a = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	o = sp_object(db);
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_rollback_a0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_rollback_a1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_rollback_b0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_rollback_b1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_rollback_ab0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_rollback_ab1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_a0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	rc = sp_commit(a);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_a1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	rc = sp_commit(a);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_b0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_b1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_a0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_a1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_b0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_b1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_n0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_n1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *c = sp_begin(db);
	t( c != NULL );
	void *b = sp_begin(db);
	t( b != NULL );

	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_n0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_n1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;
	
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(b);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_n2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(c);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_n3(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(c);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);
}

static void
transaction_c_set_commit_wait_rollback_n4(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_transaction(cx);
	rc = sp_rollback(c);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_get0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o == NULL );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 10 );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_get1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	rc = sp_rollback(a);
	t( rc == 0 );
	st_transaction(cx);

	value = 15;
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o == NULL );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 15 );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_get2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *z = sp_begin(db);

	void *a = sp_begin(db);
	t( a != NULL );
	int key = 7;
	int value = 1;

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(a, o) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	value = 2;

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	void *c = sp_begin(db);
	t( c != NULL );
	value = 3;

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(c, o) == 0 );

	void *d = sp_begin(db);
	t( d != NULL );
	value = 4;

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(d, o) == 0 );

	void *e = sp_begin(db);
	t( e != NULL );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(a, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 1 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 2 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(e, o);
	t( o == NULL );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(z, o);
	t( o == NULL );

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_rollback(tx);
	t( rc == 0 );
	st_transaction(cx);

	t( sp_rollback(d) == 0 );
	st_transaction(cx);
	t( sp_rollback(c) == 0 );
	st_transaction(cx);
	t( sp_rollback(b) == 0 );
	st_transaction(cx);
	t( sp_rollback(a) == 0 );
	st_transaction(cx);
	t( sp_rollback(e) == 0 );
	st_transaction(cx);
	t( sp_rollback(z) == 0 );
	st_transaction(cx);
}

static void
transaction_c_set_get3(stc *cx)
{
	void *db = cx->db;
	void *z = sp_begin(db);

	void *a = sp_begin(db);
	t( a != NULL );
	int key = 7;
	int value = 1;
	void *tx = sp_begin(db);

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	void *b = sp_begin(db);
	t( b != NULL );
	value = 2;
	tx = sp_begin(db);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	void *c = sp_begin(db);
	t( c != NULL );
	value = 3;
	tx = sp_begin(db);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	void *d = sp_begin(db);
	t( d != NULL );
	value = 4;
	tx = sp_begin(db);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	void *e = sp_begin(db);
	t( e != NULL );

	/* 0 */
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(b, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 1 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 2 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(a, o);
	t( o == NULL );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(z, o);
	t( o == NULL );

	/* 1 */
	t( sp_rollback(b) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 2 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(a, o);
	t( o == NULL );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(z, o);
	t( o == NULL );

	/* 2 */
	t( sp_rollback(c) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(a, o);
	t( o == NULL );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(z, o);
	t( o == NULL );

	/* 3 */
	t( sp_rollback(d) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(a, o);
	t( o == NULL );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(z, o);
	t( o == NULL );

	/* 4 */
	t( sp_rollback(e) == 0 );
	st_transaction(cx);

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(a, o);
	t( o == NULL );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(z, o);
	t( o == NULL );

	/* 6 */
	t( sp_rollback(a) == 0 );
	st_transaction(cx);
	t( sp_rollback(z) == 0 );
	st_transaction(cx);

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );
	st_transaction(cx);
}

static void
transaction_sc_set_wait(stc *cx)
{
	void *db = cx->db;
	int rc;
	int key = 7;
	void *tx = sp_begin(db);
	t( tx != NULL );

	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 2 ); /* wait */
	st_transaction(cx);

	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);
	st_transaction(cx);
}

static void
transaction_sc_get(stc *cx)
{
	void *db = cx->db;
	int rc;
	int key = 7;
	int v = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	st_transaction(cx);

	void *tx = sp_begin(db);
	t( tx != NULL );
	v = 8;

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 7 );
	sp_destroy(o);

	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	st_transaction(cx);
	t( *(int*)sp_get(o, "value", NULL) == 8 );
	sp_destroy(o);
}

static void
transaction_s_set(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	st_transaction(cx);
}

static void
transaction_s_set_get(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	st_transaction(cx);
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);
}

static void
transaction_s_set_delete_get(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(db, o) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o == NULL );
}

static void
transaction_s_set_delete_set_get(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(db, o) == 0 );
	st_transaction(cx);

	int v = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(db, o) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 8 );
	sp_destroy(o);
	st_transaction(cx);
}

st *transaction_group(void)
{
	st *group = st_def("transaction", NULL);
	st_test(group, st_def("rollback", transaction_rollback));
	st_test(group, st_def("commit", transaction_commit));
	st_test(group, st_def("set_commit", transaction_set_commit));
	st_test(group, st_def("set_get_commit", transaction_set_get_commit));
	st_test(group, st_def("set_commit_get0", transaction_set_commit_get0));
	st_test(group, st_def("set_commit_get1", transaction_set_commit_get1));
	st_test(group, st_def("set_rollback", transaction_set_rollback));
	st_test(group, st_def("set_rollback_get0", transaction_set_rollback_get0));
	st_test(group, st_def("set_rollback_get1", transaction_set_rollback_get1));
	st_test(group, st_def("set_set_commit", transaction_set_set_commit));
	st_test(group, st_def("set_set_commit_get", transaction_set_set_commit_get));
	st_test(group, st_def("set_set_get_commit", transaction_set_set_get_commit));
	st_test(group, st_def("set_set_rollback_get", transaction_set_set_rollback_get));
	st_test(group, st_def("set_delete_get_commit", transaction_set_delete_get_commit));
	st_test(group, st_def("set_delete_get_commit_get", transaction_set_delete_get_commit_get));
	st_test(group, st_def("set_delete_set_commit_get", transaction_set_delete_set_commit_get));
	st_test(group, st_def("set_delete_commit_get_set", transaction_set_delete_commit_get_set));
	st_test(group, st_def("p_set_commit", transaction_p_set_commit));
	st_test(group, st_def("p_set_get_commit", transaction_p_set_get_commit));
	st_test(group, st_def("p_set_commit_get0", transaction_p_set_commit_get0));
	st_test(group, st_def("p_set_commit_get1", transaction_p_set_commit_get1));
	st_test(group, st_def("p_set_commit_get2", transaction_p_set_commit_get2));
	st_test(group, st_def("p_set_rollback_get0", transaction_p_set_rollback_get0));
	st_test(group, st_def("p_set_rollback_get1", transaction_p_set_rollback_get1));
	st_test(group, st_def("p_set_rollback_get2", transaction_p_set_rollback_get2));
	st_test(group, st_def("c_set_commit0", transaction_c_set_commit0));
	st_test(group, st_def("c_set_commit1", transaction_c_set_commit1));
	st_test(group, st_def("c_set_commit2", transaction_c_set_commit2));
	st_test(group, st_def("c_set_commit_rollback_a0", transaction_c_set_commit_rollback_a0));
	st_test(group, st_def("c_set_commit_rollback_a1", transaction_c_set_commit_rollback_a1));
	st_test(group, st_def("c_set_commit_rollback_b0", transaction_c_set_commit_rollback_b0));
	st_test(group, st_def("c_set_commit_rollback_b1", transaction_c_set_commit_rollback_b1));
	st_test(group, st_def("c_set_commit_wait_rollback_ab0", transaction_c_set_commit_rollback_ab0));
	st_test(group, st_def("c_set_commit_wait_rollback_ab1", transaction_c_set_commit_rollback_ab1));
	st_test(group, st_def("c_set_commit_wait_a0", transaction_c_set_commit_wait_a0));
	st_test(group, st_def("c_set_commit_wait_a1", transaction_c_set_commit_wait_a1));
	st_test(group, st_def("c_set_commit_wait_b0", transaction_c_set_commit_wait_b0));
	st_test(group, st_def("c_set_commit_wait_b1", transaction_c_set_commit_wait_b1));
	st_test(group, st_def("c_set_commit_wait_rollback_a0", transaction_c_set_commit_wait_rollback_a0));
	st_test(group, st_def("c_set_commit_wait_rollback_a1", transaction_c_set_commit_wait_rollback_a1));
	st_test(group, st_def("c_set_commit_wait_rollback_b0", transaction_c_set_commit_wait_rollback_b0));
	st_test(group, st_def("c_set_commit_wait_rollback_b1", transaction_c_set_commit_wait_rollback_b1));
	st_test(group, st_def("c_set_commit_wait_n0", transaction_c_set_commit_wait_n0));
	st_test(group, st_def("c_set_commit_wait_n1", transaction_c_set_commit_wait_n1));
	st_test(group, st_def("c_set_commit_wait_rollback_n0", transaction_c_set_commit_wait_rollback_n0));
	st_test(group, st_def("c_set_commit_wait_rollback_n1", transaction_c_set_commit_wait_rollback_n1));
	st_test(group, st_def("c_set_commit_wait_rollback_n2", transaction_c_set_commit_wait_rollback_n2));
	st_test(group, st_def("c_set_commit_wait_rollback_n3", transaction_c_set_commit_wait_rollback_n3));
	st_test(group, st_def("c_set_commit_wait_rollback_n4", transaction_c_set_commit_wait_rollback_n4));
	st_test(group, st_def("c_set_get0", transaction_c_set_get0));
	st_test(group, st_def("c_set_get1", transaction_c_set_get1));
	st_test(group, st_def("c_set_get2", transaction_c_set_get2));
	st_test(group, st_def("c_set_get3", transaction_c_set_get3));
	st_test(group, st_def("sc_set_wait", transaction_sc_set_wait));
	st_test(group, st_def("sc_get", transaction_sc_get));
	st_test(group, st_def("s_set", transaction_s_set));
	st_test(group, st_def("s_set_get", transaction_s_set_get));
	st_test(group, st_def("s_set_delete_get", transaction_s_set_delete_get));
	st_test(group, st_def("s_set_delete_set_get", transaction_s_set_delete_set_get));
	return group;
}
