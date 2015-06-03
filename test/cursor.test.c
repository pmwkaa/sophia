
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libst.h>
#include <sophia.h>

static void
cursor_empty_gte(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_empty_gt(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_empty_lte(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", "<=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_empty_lt(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", "<") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_gte(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_gt(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_lte(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", "<=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_lt(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", "<") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gte0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gte1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gte2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gte3(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 15;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gte4(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 73;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 80;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 90;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 79;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 80 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 90 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gte5(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 0;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gt0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gt1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gt2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", ">") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lte0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lte1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lte2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lte3(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 5;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lte4(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 20;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<=") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lt0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lt1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lt2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lt3(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 2;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_lt4(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	key = 20;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	t( sp_set(pos, "order", "<") == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	sp_destroy(o);
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	sp_destroy(o);
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_pos_gte_range(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "order", ">=") == 0 );
		void *c = sp_cursor(db, o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i );
		t( *(int*)sp_get(v, "value", NULL) == i );
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_transaction(cx);
}

static void
cursor_pos_gt_range(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	i = 0;
	while (i < (385 - 1)) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "order", ">") == 0 );
		void *c = sp_cursor(db, o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i + 1);
		t( *(int*)sp_get(v, "value", NULL) == i + 1);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_transaction(cx);
}

static void
cursor_pos_lte_range(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "order", "<=") == 0 );
		void *c = sp_cursor(db, o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i);
		t( *(int*)sp_get(v, "value", NULL) == i);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_transaction(cx);
}

static void
cursor_pos_lt_range(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	i = 1;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "order", "<") == 0 );
		void *c = sp_cursor(db, o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i - 1);
		t( *(int*)sp_get(v, "value", NULL) == i - 1);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_transaction(cx);
}

static void
cursor_pos_gte_random(stc *cx)
{
	void *db = cx->db;
	int rc;
	unsigned int seed = time(NULL);
	srand(seed);
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int i = 0;
	while (i < 270) {
		int key = rand();
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	srand(seed);
	i = 0;
	while (i < 270) {
		int key = rand();
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "order", ">=") == 0 );
		void *c = sp_cursor(db, o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == key);
		t( *(int*)sp_get(v, "value", NULL) == i);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_transaction(cx);
}

static void
cursor_pos_lte_random(stc *cx)
{
	void *db = cx->db;
	int rc;
	unsigned int seed = time(NULL);
	srand(seed);
	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int i = 0;
	while (i < 403) {
		int key = rand();
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	srand(seed);
	i = 0;
	while (i < 403) {
		int key = rand();
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "order", "<=") == 0 );
		void *c = sp_cursor(db, o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == key);
		t( *(int*)sp_get(v, "value", NULL) == i);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_transaction(cx);
}

static void
cursor_consistency0(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	void *tx = sp_begin(cx->env);
	int key = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	st_transaction(cx);
	t( rc == 0 );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency1(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	st_transaction(cx);

	tx = sp_begin(cx->env);
	t( tx != NULL );
	key = 0;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	key = 19;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 7 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 8 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 9 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency2(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int k = 1;
	int v = 2;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 1;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency3(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int k = 1;
	int v = 2;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 1;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 2;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency4(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int k = 1;
	int v = 2;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 1;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 2;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 3;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency5(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int v = 2;
	int k;
	k = 1;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 2;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency6(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int v = 2;
	int k;
	k = 1;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 2;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 3;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency7(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int v = 2;
	int k;
	k = 1;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 0;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 5;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 7;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistency8(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int v = 2;
	int k;
	k = 1;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 0;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 0;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 5;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 7;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
}

static void
cursor_consistency9(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int v = 2;
	int k;
	k = 1;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 0;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 5;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 6;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 7;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 8;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	c = sp_cursor(db, o);
	t( c != NULL );

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 0 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 5 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 7 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 8 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_destroy(o) == 0 );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	st_transaction(cx);
}

static void
cursor_consistencyN(stc *cx)
{
	void *db = cx->db;
	int rc;
	void *tx = sp_begin(cx->env);
	int k;
	int v = 2;
	k = 1;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 0;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	tx = sp_begin(cx->env);
	t( tx != NULL );
	k = 4;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c2 = sp_cursor(db, o);
	t( c2 != NULL );

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	o = sp_get(c);
	t( o == NULL );
	sp_destroy(c);

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 0 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );
	st_transaction(cx);

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	t( sp_destroy(o) == 0 );

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_destroy(o) == 0 );
	t( sp_get(c2) == NULL );
	t( sp_destroy(c2) == 0 );
}

static void
cursor_consistency_rewrite0(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &v, sizeof(v)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0);
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c1 = sp_cursor(db, o);

	tx = sp_begin(cx->env);
	v = 20;
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &v, sizeof(v)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0);
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c2 = sp_cursor(db, o);

	t( sp_get(c0) == NULL );

	i = 0;
	while ((o = sp_get(c1))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 15 );
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);
	st_transaction(cx);

	i = 0;
	while ((o = sp_get(c2))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 20 );
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );
}

static void
cursor_consistency_rewrite1(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &v, sizeof(v)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c1 = sp_cursor(db, o);

	tx = sp_begin(cx->env);
	v = 20;
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &v, sizeof(v)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	t( sp_get(c0) == NULL );

	i = 0;
	while ((o = sp_get(c1))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 15 );
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);

	i = 0;
	while (i < 385) {
		void *ckey = sp_object(db);
		t( sp_set(ckey, "key", &i, sizeof(i)) == 0 );
		t( sp_set(ckey, "order", ">=") == 0 );

		void *c2 = sp_cursor(db, ckey);
		t( c2 != NULL );
		void *o = sp_get(c2);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 20 );
		t( sp_destroy(o) == 0 );
		t( sp_destroy(c2) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c1) == 0 );
}

static void
cursor_consistency_rewrite2(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(cx->env);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &v, sizeof(v)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c1 = sp_cursor(db, o);
	v = 20;
	i = 0;
	while ((o = sp_get(c1))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 15 );
		t( sp_destroy(o) == 0 );

		tx = sp_begin(cx->env);
		o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &v, sizeof(v)) == 0 );
		t( sp_set(tx, o) == 0 );
		t( sp_commit(tx) == 0 );
		i++;
	}
	t(i == 385);
	st_transaction(cx);

	t( sp_get(c0) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c2 = sp_cursor(db, o);
	i = 0;
	while ((o = sp_get(c2))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 20 );
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);
	st_transaction(cx);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );
}

static void
cursor_consistency_delete0(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(cx->env);
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	tx = sp_begin(cx->env);
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_delete(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	t( sp_get(c0) == NULL );

	t( sp_destroy(c0) == 0 );
}

static void
cursor_consistency_delete1(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(cx->env);
	int i = 0;
	while (i < 385) {
		o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_transaction(cx);

	tx = sp_begin(cx->env);
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">=") == 0 );
	void *c1 = sp_cursor(db, o);
	while ((o = sp_get(c1))) {
		t( sp_delete(tx, o) == 0 );
		t( sp_destroy(o) == 0 );
	}
	t( sp_commit(tx) == 0 );
	t( sp_destroy(c1) == 0 );
	st_transaction(cx);

	t( sp_get(c0) == NULL );
	t( sp_destroy(c0) == 0 );
}

stgroup *cursor_group(void)
{
	stgroup *group = st_group("cursor");
	st_groupadd(group, st_test("empty_gte", cursor_empty_gte));
	st_groupadd(group, st_test("empty_gt", cursor_empty_gt));
	st_groupadd(group, st_test("empty_lte", cursor_empty_lte));
	st_groupadd(group, st_test("empty_lt", cursor_empty_lt));
	st_groupadd(group, st_test("gte", cursor_gte));
	st_groupadd(group, st_test("gt", cursor_gt));
	st_groupadd(group, st_test("lte", cursor_lte));
	st_groupadd(group, st_test("lt", cursor_lt));
	st_groupadd(group, st_test("pos_gte0", cursor_pos_gte0));
	st_groupadd(group, st_test("pos_gte1", cursor_pos_gte1));
	st_groupadd(group, st_test("pos_gte2", cursor_pos_gte2));
	st_groupadd(group, st_test("pos_gte3", cursor_pos_gte3));
	st_groupadd(group, st_test("pos_gte4", cursor_pos_gte4));
	st_groupadd(group, st_test("pos_gte5", cursor_pos_gte5));
	st_groupadd(group, st_test("pos_gt0", cursor_pos_gt0));
	st_groupadd(group, st_test("pos_gt1", cursor_pos_gt1));
	st_groupadd(group, st_test("pos_gt2", cursor_pos_gt2));
	st_groupadd(group, st_test("pos_lte0", cursor_pos_lte0));
	st_groupadd(group, st_test("pos_lte1", cursor_pos_lte1));
	st_groupadd(group, st_test("pos_lte2", cursor_pos_lte2));
	st_groupadd(group, st_test("pos_lte3", cursor_pos_lte3));
	st_groupadd(group, st_test("pos_lte4", cursor_pos_lte4));
	st_groupadd(group, st_test("pos_lt0", cursor_pos_lt0));
	st_groupadd(group, st_test("pos_lt1", cursor_pos_lt1));
	st_groupadd(group, st_test("pos_lt2", cursor_pos_lt2));
	st_groupadd(group, st_test("pos_lt3", cursor_pos_lt3));
	st_groupadd(group, st_test("pos_lt4", cursor_pos_lt4));
	st_groupadd(group, st_test("pos_gte_range", cursor_pos_gte_range));
	st_groupadd(group, st_test("pos_gt_range", cursor_pos_gt_range));
	st_groupadd(group, st_test("pos_lte_range", cursor_pos_lte_range));
	st_groupadd(group, st_test("pos_lt_range", cursor_pos_lt_range));
	st_groupadd(group, st_test("pos_gte_random", cursor_pos_gte_random));
	st_groupadd(group, st_test("pos_lte_random", cursor_pos_lte_random));
	st_groupadd(group, st_test("consistency0", cursor_consistency0));
	st_groupadd(group, st_test("consistency1", cursor_consistency1));
	st_groupadd(group, st_test("consistency2", cursor_consistency2));
	st_groupadd(group, st_test("consistency3", cursor_consistency3));
	st_groupadd(group, st_test("consistency4", cursor_consistency4));
	st_groupadd(group, st_test("consistency5", cursor_consistency5));
	st_groupadd(group, st_test("consistency6", cursor_consistency6));
	st_groupadd(group, st_test("consistency7", cursor_consistency7));
	st_groupadd(group, st_test("consistency8", cursor_consistency8));
	st_groupadd(group, st_test("consistency9", cursor_consistency9));
	st_groupadd(group, st_test("consistencyN", cursor_consistencyN));
	st_groupadd(group, st_test("consistency_rewrite0", cursor_consistency_rewrite0));
	st_groupadd(group, st_test("consistency_rewrite1", cursor_consistency_rewrite1));
	st_groupadd(group, st_test("consistency_rewrite2", cursor_consistency_rewrite2));
	st_groupadd(group, st_test("consistency_delete0", cursor_consistency_delete0));
	st_groupadd(group, st_test("consistency_delete1", cursor_consistency_delete1));
	return group;
}
