
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
transaction_rollback(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	st_phase();
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_commit(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	st_phase();
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_commit(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_get_commit(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_commit_get0(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_commit_get1(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = 0;
	while (key < 10) {
		void *o = st_object(key, key);
		t( sp_set(tx, o) == 0 );
		key++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 0;
	tx = sp_begin(st_r.env);
	while (key < 10) {
		void *o = st_object(key, key);
		o = sp_get(tx, o);
		t( o != NULL );
		t( *(int*)sp_getstring(o, "value", NULL) == key );
		sp_destroy(o);
		key++;
	}
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_rollback(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_rollback_get0(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_rollback_get1(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = 0;
	while (key < 10) {
		void *o = st_object(key, key);
		t( sp_set(tx, o) == 0 );
		key++;
	}
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
	tx = sp_begin(st_r.env);
	key = 0;
	while (key < 10) {
		void *o = st_object(key, key);
		o = sp_get(tx, o);
		t( o == NULL );
		key++;
	}
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_set_commit(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;
	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	value = st_seed();
	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_set_get_commit(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;
	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	value = st_seed();
	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_set_commit_get(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;

	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );

	value = st_seed();
	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );

	rc = sp_commit(tx);
	t( rc == 0 );

	st_phase();
	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_set_rollback_get(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;
	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	value = st_seed();
	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_delete_get_commit(void)
{
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;
	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	o = st_object(key, key);
	t( sp_delete(tx, o) == 0 );
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_set_delete_get_commit_get(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;
	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	o = st_object(key, key);
	t( sp_delete(tx, o) == 0 );
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	o = st_object(key, key);
	o = sp_get(db, o);
	t( o == NULL );
	st_phase();
}

static void
transaction_set_delete_set_commit_get(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;

	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );

	o = st_object(key, key);
	t( sp_delete(tx, o) == 0 );

	value = st_seed();
	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );

	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value );
	sp_destroy(o);
	st_phase();
}

static void
transaction_set_delete_commit_get_set(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int key = st_seed();
	int value = key;

	void *o = st_object(key, value);
	t( sp_set(tx, o) == 0 );

	o = st_object(key, key);
	t( sp_delete(tx, o) == 0 );

	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(db, o);
	t( o == NULL );

	o = st_object(key, value);
	t( sp_set(db, o) == 0 );

	o = st_object(key, key);
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value );
	sp_destroy(o);
	st_phase();
}

static void
transaction_p_set_commit(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int value = 10;
	int key_a = st_seed();

	void *o = st_object(key_a, value);
	t( sp_set(a, o) == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	int key_b = st_seed();
	o = st_object(key_b, value);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_p_set_get_commit(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int value_a = 10;
	int key_a = st_seed();

	void *o = st_object(key_a, value_a);
	t( sp_set(a, o) == 0 );

	o = st_object(key_a, key_a);
	o = sp_get(a, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value_a );
	sp_destroy(o);

	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	int value_b = 15;
	int key_b = st_seed();

	o = st_object(key_b, value_b);
	t( sp_set(b, o) == 0 );

	o = st_object(key_b, key_b);
	o = sp_get(b, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value_b );
	sp_destroy(o);

	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_p_set_commit_get0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key_a = st_seed();
	int value_a = 10;

	void *o = st_object(key_a, value_a);
	t( sp_set(a, o) == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	int key_b = st_seed();
	int value_b = 15;
	o = st_object(key_b, value_b);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key_a, key_a);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value_a );
	sp_destroy(o);

	o = st_object(key_b, key_b);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value_b );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );
	st_phase();
}

static void
transaction_p_set_commit_get1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );

	int key_a = st_seed();
	int value_a = 10;
	void *o = st_object(key_a, value_a);
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	int key_b = st_seed();
	int value_b = 15;
	o = st_object(key_b, value_b);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key_a, value_a);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value_a );
	sp_destroy(o);

	o = st_object(key_b, value_b);
	t( *(int*)sp_getstring(o, "value", NULL) == value_b );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );
	st_phase();
}

static void
transaction_p_set_commit_get2(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );

	int key_b = st_seed();
	int value_b = 15;

	void *o = st_object(key_b, value_b);
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	int key_a = st_seed();
	int value_a = 10;

	o = st_object(key_a, value_a);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key_a, key_a);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value_a );
	sp_destroy(o);

	o = st_object(key_b, key_b);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value_b );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );
	st_phase();
}

static void
transaction_p_set_rollback_get0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );

	int key_a = st_seed();
	int value_a = 10;

	void *o = st_object(key_a, value_a);
	t( sp_set(a, o) == 0 );
	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();

	int key_b = st_seed();
	int value_b = 15;
	o = st_object(key_b, value_b);
	t( sp_set(b, o) == 0 );
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key_a, key_a);
	o = sp_get(tx, o);
	t( o == NULL );

	o = st_object(key_b, key_b);
	o = sp_get(tx, o);
	t( o == NULL );
	t( sp_destroy(tx) == 0 );
	st_phase();
}

static void
transaction_p_set_rollback_get1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );

	int key_a = st_seed();
	int value_a = 10;
	void *o = st_object(key_a, value_a);
	t( sp_set(a, o) == 0 );

	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();

	int key_b = st_seed();
	int value_b = 15;
	o = st_object(key_b, value_b);
	t( sp_set(b, o) == 0 );
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key_a, key_a);
	o = sp_get(tx, o);
	t( o == NULL );
	o = st_object(key_b, key_b);
	o = sp_get(tx, o);
	t( o == NULL );
	t( sp_destroy(tx) == 0 );
	st_phase();
}

static void
transaction_p_set_rollback_get2(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );

	int key_b = st_seed();
	int value_b = 15;

	void *o = st_object(key_b, value_b);
	t( sp_set(b, o) == 0 );
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();

	int key_a = st_seed();
	int value_a = 10;
	o = st_object(key_a, value_a);
	t( sp_set(a, o) == 0 );
	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key_a, key_a);
	o = sp_get(tx, o);
	t( o == NULL );
	o = st_object(key_b, key_b);
	o = sp_get(tx, o);
	t( o == NULL );
	t( sp_destroy(tx) == 0 );
	st_phase();
}

static void
transaction_c_set_commit0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key = st_seed();
	int value = 10;
	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit2(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_rollback_a0(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_rollback_a1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_rollback_b0(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );

	st_phase();
	o = st_object(key, value);
	t( sp_set(b, o) == 0 );
	rc = sp_destroy(b);
	t( rc == 0 );

	st_phase();
}

static void
transaction_c_set_commit_rollback_b1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_rollback_ab0(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );
	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_rollback_ab1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_wait_a0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	rc = sp_commit(a);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_a1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	rc = sp_commit(a);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_b0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_b1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_a0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_a1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_b0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_b1(void)
{
	int rc;
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_commit_wait_n0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *c = sp_begin(st_r.env);
	t( c != NULL );

	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_n1(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *c = sp_begin(st_r.env);
	t( c != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );

	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_n0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *c = sp_begin(st_r.env);
	t( c != NULL );

	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_n1(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *c = sp_begin(st_r.env);
	t( c != NULL );

	int key = st_seed();
	int value = 10;
	
	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(b);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_n2(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *c = sp_begin(st_r.env);
	t( c != NULL );

	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(c);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_n3(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *c = sp_begin(st_r.env);
	t( c != NULL );

	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(c);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();
}

static void
transaction_c_set_commit_wait_rollback_n4(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	void *c = sp_begin(st_r.env);
	t( c != NULL );

	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	st_phase();
	rc = sp_destroy(c);
	t( rc == 0 );
	st_phase();
	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_get0(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(b, o);
	t( o == NULL );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_get1(void)
{
	int rc;
	void *a = sp_begin(st_r.env);
	t( a != NULL );
	void *b = sp_begin(st_r.env);
	t( b != NULL );
	int key = st_seed();
	int value = 10;

	void *o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	rc = sp_destroy(a);
	t( rc == 0 );
	st_phase();

	value = 15;
	o = st_object(key, key);
	o = sp_get(b, o);
	t( o == NULL );

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
}

static void
transaction_c_set_get2(void)
{
	int rc;
	void *z = sp_begin(st_r.env);

	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 1;

	void *o;
	o = st_object(key, value);
	t( sp_set(a, o) == 0 );

	void *b = sp_begin(st_r.env);
	t( b != NULL );
	value = 2;

	o = st_object(key, value);
	t( sp_set(b, o) == 0 );

	void *c = sp_begin(st_r.env);
	t( c != NULL );
	value = 3;

	o = st_object(key, value);
	t( sp_set(c, o) == 0 );

	void *d = sp_begin(st_r.env);
	t( d != NULL );
	value = 4;

	o = st_object(key, value);
	t( sp_set(d, o) == 0 );

	void *e = sp_begin(st_r.env);
	t( e != NULL );

	o = st_object(key, key);
	o = sp_get(a, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 1 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(b, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 2 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(e, o);
	t( o == NULL );

	o = st_object(key, key);
	o = sp_get(z, o);
	t( o == NULL );

	void *tx = sp_begin(st_r.env);

	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_destroy(tx);
	t( rc == 0 );
	st_phase();

	t( sp_destroy(d) == 0 );
	st_phase();
	t( sp_destroy(c) == 0 );
	st_phase();
	t( sp_destroy(b) == 0 );
	st_phase();
	t( sp_destroy(a) == 0 );
	st_phase();
	t( sp_destroy(e) == 0 );
	st_phase();
	t( sp_destroy(z) == 0 );
	st_phase();
}

static void
transaction_c_set_get3(void)
{
	void *z = sp_begin(st_r.env);

	void *a = sp_begin(st_r.env);
	t( a != NULL );
	int key = st_seed();
	int value = 1;
	void *tx = sp_begin(st_r.env);

	void *o;
	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_phase();

	void *b = sp_begin(st_r.env);
	t( b != NULL );
	value = 2;
	tx = sp_begin(st_r.env);

	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_phase();

	void *c = sp_begin(st_r.env);
	t( c != NULL );
	value = 3;
	tx = sp_begin(st_r.env);

	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_phase();

	void *d = sp_begin(st_r.env);
	t( d != NULL );
	value = 4;
	tx = sp_begin(st_r.env);

	o = st_object(key, value);
	t( sp_set(tx, o) == 0 );
	t( sp_commit(tx) == 0 );
	st_phase();

	void *e = sp_begin(st_r.env);
	t( e != NULL );

	/* 0 */
	o = st_object(key, key);
	o = sp_get(b, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 1 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 2 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );

	o = st_object(key, key);
	o = sp_get(a, o);
	t( o == NULL );

	o = st_object(key, key);
	o = sp_get(z, o);
	t( o == NULL );

	/* 1 */
	t( sp_destroy(b) == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(c, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 2 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(a, o);
	t( o == NULL );

	o = st_object(key, key);
	o = sp_get(z, o);
	t( o == NULL );

	/* 2 */
	t( sp_destroy(c) == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(d, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 3 );
	sp_destroy(o);

	o = st_object(key, key);
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );

	o = st_object(key, key);
	o = sp_get(a, o);
	t( o == NULL );

	o = st_object(key, key);
	o = sp_get(z, o);
	t( o == NULL );

	/* 3 */
	t( sp_destroy(d) == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(e, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);

	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(a, o);
	t( o == NULL );

	o = st_object(key, key);
	o = sp_get(z, o);
	t( o == NULL );

	/* 4 */
	t( sp_destroy(e) == 0 );
	st_phase();

	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(a, o);
	t( o == NULL );

	o = st_object(key, key);
	o = sp_get(z, o);
	t( o == NULL );

	/* 6 */
	t( sp_destroy(a) == 0 );
	st_phase();
	t( sp_destroy(z) == 0 );
	st_phase();

	tx = sp_begin(st_r.env);
	o = st_object(key, key);
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_destroy(tx) == 0 );
	st_phase();
}

static void
transaction_sc_set_wait(void)
{
	void *db = st_r.db;
	int rc;
	int key = st_seed();
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );

	void *o;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );

	o = st_object(key, key);
	t( sp_set(db, o) == 2 ); /* wait */
	st_phase();

	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);
	st_phase();
}

static void
transaction_sc_get(void)
{
	void *db = st_r.db;
	int rc;
	int key = st_seed();
	int v = 7;

	void *o;
	o = st_object(key, v);
	t( sp_set(db, o) == 0 );
	st_phase();

	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	v = 8;

	o = st_object(key, v);
	t( sp_set(tx, o) == 0 );

	o = st_object(key, v);
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 7 );
	sp_destroy(o);

	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = st_object(key, v);
	o = sp_get(db, o);
	t( o != NULL );
	st_phase();
	t( *(int*)sp_getstring(o, "value", NULL) == 8 );
	sp_destroy(o);
}

static void
transaction_s_set(void)
{
	void *db = st_r.db;
	int key = st_seed();
	void *o;
	o = st_object(key, key);
	t( sp_set(db, o) == 0 );
	st_phase();
}

static void
transaction_s_set_get(void)
{
	void *db = st_r.db;
	int key = st_seed();
	void *o;
	o = st_object(key, key);
	t( sp_set(db, o) == 0 );
	st_phase();
	o = st_object(key, key);
	o = sp_get(db, o);
	t( o != NULL );
	st_phase();
	t( *(int*)sp_getstring(o, "value", NULL) == key );
	sp_destroy(o);
}

static void
transaction_s_set_delete_get(void)
{
	void *db = st_r.db;
	int key = st_seed();
	void *o;
	o = st_object(key, key);
	t( sp_set(db, o) == 0 );
	st_phase();
	o = st_object(key, key);
	t( sp_delete(db, o) == 0 );
	st_phase();
	o = st_object(key, key);
	o = sp_get(db, o);
	t( o == NULL );
}

static void
transaction_s_set_delete_set_get(void)
{
	void *db = st_r.db;
	int key = st_seed();

	void *o;
	o = st_object(key, key);
	t( sp_set(db, o) == 0 );

	st_phase();
	o = st_object(key, key);
	t( sp_delete(db, o) == 0 );
	st_phase();

	int v = 8;
	o = st_object(key, v);
	t( sp_set(db, o) == 0 );
	st_phase();

	o = st_object(key, key);
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == 8 );
	sp_destroy(o);
	st_phase();
}

stgroup *transaction_group(void)
{
	stgroup *group = st_group("transaction");
	st_groupadd(group, st_test("rollback", transaction_rollback));
	st_groupadd(group, st_test("commit", transaction_commit));
	st_groupadd(group, st_test("set_commit", transaction_set_commit));
	st_groupadd(group, st_test("set_get_commit", transaction_set_get_commit));
	st_groupadd(group, st_test("set_commit_get0", transaction_set_commit_get0));
	st_groupadd(group, st_test("set_commit_get1", transaction_set_commit_get1));
	st_groupadd(group, st_test("set_rollback", transaction_set_rollback));
	st_groupadd(group, st_test("set_rollback_get0", transaction_set_rollback_get0));
	st_groupadd(group, st_test("set_rollback_get1", transaction_set_rollback_get1));
	st_groupadd(group, st_test("set_set_commit", transaction_set_set_commit));
	st_groupadd(group, st_test("set_set_commit_get", transaction_set_set_commit_get));
	st_groupadd(group, st_test("set_set_get_commit", transaction_set_set_get_commit));
	st_groupadd(group, st_test("set_set_rollback_get", transaction_set_set_rollback_get));
	st_groupadd(group, st_test("set_delete_get_commit", transaction_set_delete_get_commit));
	st_groupadd(group, st_test("set_delete_get_commit_get", transaction_set_delete_get_commit_get));
	st_groupadd(group, st_test("set_delete_set_commit_get", transaction_set_delete_set_commit_get));
	st_groupadd(group, st_test("set_delete_commit_get_set", transaction_set_delete_commit_get_set));
	st_groupadd(group, st_test("p_set_commit", transaction_p_set_commit));
	st_groupadd(group, st_test("p_set_get_commit", transaction_p_set_get_commit));
	st_groupadd(group, st_test("p_set_commit_get0", transaction_p_set_commit_get0));
	st_groupadd(group, st_test("p_set_commit_get1", transaction_p_set_commit_get1));
	st_groupadd(group, st_test("p_set_commit_get2", transaction_p_set_commit_get2));
	st_groupadd(group, st_test("p_set_rollback_get0", transaction_p_set_rollback_get0));
	st_groupadd(group, st_test("p_set_rollback_get1", transaction_p_set_rollback_get1));
	st_groupadd(group, st_test("p_set_rollback_get2", transaction_p_set_rollback_get2));
	st_groupadd(group, st_test("c_set_commit0", transaction_c_set_commit0));
	st_groupadd(group, st_test("c_set_commit1", transaction_c_set_commit1));
	st_groupadd(group, st_test("c_set_commit2", transaction_c_set_commit2));
	st_groupadd(group, st_test("c_set_commit_rollback_a0", transaction_c_set_commit_rollback_a0));
	st_groupadd(group, st_test("c_set_commit_rollback_a1", transaction_c_set_commit_rollback_a1));
	st_groupadd(group, st_test("c_set_commit_rollback_b0", transaction_c_set_commit_rollback_b0));
	st_groupadd(group, st_test("c_set_commit_rollback_b1", transaction_c_set_commit_rollback_b1));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_ab0", transaction_c_set_commit_rollback_ab0));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_ab1", transaction_c_set_commit_rollback_ab1));
	st_groupadd(group, st_test("c_set_commit_wait_a0", transaction_c_set_commit_wait_a0));
	st_groupadd(group, st_test("c_set_commit_wait_a1", transaction_c_set_commit_wait_a1));
	st_groupadd(group, st_test("c_set_commit_wait_b0", transaction_c_set_commit_wait_b0));
	st_groupadd(group, st_test("c_set_commit_wait_b1", transaction_c_set_commit_wait_b1));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_a0", transaction_c_set_commit_wait_rollback_a0));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_a1", transaction_c_set_commit_wait_rollback_a1));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_b0", transaction_c_set_commit_wait_rollback_b0));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_b1", transaction_c_set_commit_wait_rollback_b1));
	st_groupadd(group, st_test("c_set_commit_wait_n0", transaction_c_set_commit_wait_n0));
	st_groupadd(group, st_test("c_set_commit_wait_n1", transaction_c_set_commit_wait_n1));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_n0", transaction_c_set_commit_wait_rollback_n0));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_n1", transaction_c_set_commit_wait_rollback_n1));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_n2", transaction_c_set_commit_wait_rollback_n2));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_n3", transaction_c_set_commit_wait_rollback_n3));
	st_groupadd(group, st_test("c_set_commit_wait_rollback_n4", transaction_c_set_commit_wait_rollback_n4));
	st_groupadd(group, st_test("c_set_get0", transaction_c_set_get0));
	st_groupadd(group, st_test("c_set_get1", transaction_c_set_get1));
	st_groupadd(group, st_test("c_set_get2", transaction_c_set_get2));
	st_groupadd(group, st_test("c_set_get3", transaction_c_set_get3));
	st_groupadd(group, st_test("sc_set_wait", transaction_sc_set_wait));
	st_groupadd(group, st_test("sc_get", transaction_sc_get));
	st_groupadd(group, st_test("s_set", transaction_s_set));
	st_groupadd(group, st_test("s_set_get", transaction_s_set_get));
	st_groupadd(group, st_test("s_set_delete_get", transaction_s_set_delete_get));
	st_groupadd(group, st_test("s_set_delete_set_get", transaction_s_set_delete_set_get));
	return group;
}
