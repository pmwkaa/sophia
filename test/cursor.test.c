
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
cursor_empty_gte(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_empty_gt(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_empty_lte(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", "<=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_empty_lt(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", "<", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_gte(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_gt(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_lte(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", "<=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_lt(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", "<", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gte0(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 7;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gte1(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 8;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gte2(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 9;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gte3(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 15;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gte4(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 73;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 80;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 90;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 79;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 80, 80);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 90, 90);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gte5(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 0;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gt0(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 7;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gt1(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 8;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gt2(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 9;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", ">", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lte0(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 9;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lte1(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 8;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lte2(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 7;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lte3(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 5;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lte4(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 20;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<=", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lt0(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 9;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lt1(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 8;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lt2(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 7;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lt3(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 2;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_lt4(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	key = 20;
	void *pos = st_object(key, key);
	t( sp_setstring(pos, "order", "<", 0) == 0 );
	void *c = sp_cursor(db, pos);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	sp_destroy(o);
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_pos_gte_range(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	i = 0;
	while (i < 385) {
		void *pos = st_object(i, i);
		t( sp_setstring(pos, "order", ">=", 0) == 0 );
		void *c = sp_cursor(db, pos);
		t( c != NULL );
		void *v = sp_get(c, NULL);
		t( v != NULL );
		st_object_is(v, i, i);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_phase();
}

static void
cursor_pos_gt_range(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	i = 0;
	while (i < (385 - 1)) {
		void *pos = st_object(i, i);
		t( sp_setstring(pos, "order", ">", 0) == 0 );
		void *c = sp_cursor(db, pos);
		t( c != NULL );
		void *v = sp_get(c, NULL);
		t( v != NULL );
		st_object_is(v, i + 1, i + 1);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_phase();
}

static void
cursor_pos_lte_range(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	i = 0;
	while (i < 385) {
		void *pos = st_object(i, i);
		t( sp_setstring(pos, "order", "<=", 0) == 0 );
		void *c = sp_cursor(db, pos);
		t( c != NULL );
		void *v = sp_get(c, NULL);
		t( v != NULL );
		st_object_is(v, i, i);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_phase();
}

static void
cursor_pos_lt_range(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	i = 1;
	while (i < 385) {
		void *pos = st_object(i, i);
		t( sp_setstring(pos, "order", "<", 0) == 0 );
		void *c = sp_cursor(db, pos);
		t( c != NULL );
		void *v = sp_get(c, NULL);
		t( v != NULL );
		st_object_is(v, i - 1, i - 1);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_phase();
}

static void
cursor_pos_gte_random(void)
{
	void *db = st_r.db;
	int rc;
	unsigned int seed = time(NULL);
	srand(seed);
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int i = 0;
	while (i < 270) {
		int key = rand();
		void *o = st_object(key, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	srand(seed);
	i = 0;
	while (i < 270) {
		int key = rand();
		void *pos = st_object(key, key);
		t( sp_setstring(pos, "order", ">=", 0) == 0 );
		void *c = sp_cursor(db, pos);
		t( c != NULL );
		void *v = sp_get(c, NULL);
		t( v != NULL );
		st_object_is(v, key, i);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_phase();
}

static void
cursor_pos_lte_random(void)
{
	void *db = st_r.db;
	int rc;
	unsigned int seed = time(NULL);
	srand(seed);
	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int i = 0;
	while (i < 403) {
		int key = rand();
		void *o = st_object(key, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	srand(seed);
	i = 0;
	while (i < 403) {
		int key = rand();
		void *pos = st_object(key, key);
		t( sp_setstring(pos, "order", "<=", 0) == 0 );
		void *c = sp_cursor(db, pos);
		t( c != NULL );
		void *v = sp_get(c, NULL);
		t( v != NULL );
		st_object_is(v, key, i);
		t( sp_destroy(v) == 0 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	st_phase();
}

static void
cursor_consistency0(void)
{
	void *db = st_r.db;
	int rc;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	void *tx = sp_begin(st_r.env);
	int key = 7;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	st_phase();
	t( rc == 0 );
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency1(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int key = 7;
	void *o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 8;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 9;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	st_phase();

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	key = 0;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	key = 19;
	o = st_object(key, key);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 7);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 8);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 9, 9);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency2(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int k = 1;
	int v = 2;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 1;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 2, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 3, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency3(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int k = 1;
	int v = 2;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 1;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 2;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 2, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 3, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency4(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int k = 1;
	int v = 2;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 1;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 2;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 3;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 2, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 3, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency5(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int v = 2;
	int k;
	k = 1;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 2;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 4, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 6, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency6(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int v = 2;
	int k;
	k = 1;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 2;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 3;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = sp_object(db);
	t( o != NULL );
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 4, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 6, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency7(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int v = 2;
	int k;
	k = 1;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 0;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 5;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 7;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 4, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 6, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistency8(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int v = 2;
	int k;
	k = 1;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 0;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 4, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 0;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 5;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 6, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 7;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );
}

static void
cursor_consistency9(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int v = 2;
	int k;
	k = 1;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 0;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 4;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 4, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 5;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 6;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 6, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 7;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 8;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	c = sp_cursor(db, o);
	t( c != NULL );

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 0, 3);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 4, 3);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 5, 3);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 6, 3);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 7, 3);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 8, 3);
	t( sp_destroy(o) == 0 );
	t( sp_get(c, NULL) == NULL );
	t( sp_destroy(c) == 0 );
	st_phase();
}

static void
cursor_consistencyN(void)
{
	void *db = st_r.db;
	int rc;
	void *tx = sp_begin(st_r.env);
	int k;
	int v = 2;
	k = 1;
	void *o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 2;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c = sp_cursor(db, o);
	t( c != NULL );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 0;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	k = 4;
	v = 3;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	tx = sp_begin(st_r.env);
	t( tx != NULL );
	k = 4;
	o = st_object(k, v);
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c2 = sp_cursor(db, o);
	t( c2 != NULL );

	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 2, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o != NULL );
	st_object_is(o, 3, 2);
	t( sp_destroy(o) == 0 );
	o = sp_get(c, NULL);
	t( o == NULL );
	sp_destroy(c);

	o = sp_get(c2, NULL);
	t( o != NULL );
	st_object_is(o, 0, 2);
	t( sp_destroy(o) == 0 );

	o = sp_get(c2, NULL);
	t( o != NULL );
	st_object_is(o, 1, 2);
	t( sp_destroy(o) == 0 );

	o = sp_get(c2, NULL);
	t( o != NULL );
	st_object_is(o, 2, 2);
	t( sp_destroy(o) == 0 );
	st_phase();

	o = sp_get(c2, NULL);
	t( o != NULL );
	st_object_is(o, 3, 2);
	t( sp_destroy(o) == 0 );

	o = sp_get(c2, NULL);
	t( o != NULL );
	st_object_is(o, 4, 3);
	t( sp_destroy(o) == 0 );
	t( sp_get(c2, NULL) == NULL );
	t( sp_destroy(c2) == 0 );
}

static void
cursor_consistency_rewrite0(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, v);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0);
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c1 = sp_cursor(db, o);

	tx = sp_begin(st_r.env);
	v = 20;
	i = 0;
	while (i < 385) {
		void *o = st_object(i, v);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0);
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c2 = sp_cursor(db, o);

	t( sp_get(c0, NULL) == NULL );

	i = 0;
	while ((o = sp_get(c1, NULL))) {
		st_object_is(o, i, 15);
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);
	st_phase();

	i = 0;
	while ((o = sp_get(c2, NULL))) {
		st_object_is(o, i, 20);
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );
}

static void
cursor_consistency_rewrite1(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, v);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c1 = sp_cursor(db, o);

	tx = sp_begin(st_r.env);
	v = 20;
	i = 0;
	while (i < 385) {
		void *o = st_object(i, v);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_phase();

	t( sp_get(c0, NULL) == NULL );

	i = 0;
	while ((o = sp_get(c1, NULL))) {
		st_object_is(o, i, 15);
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);

	i = 0;
	while (i < 385) {
		void *ckey = st_object(i, i);
		t( sp_setstring(ckey, "order", ">=", 0) == 0 );

		void *c2 = sp_cursor(db, ckey);
		t( c2 != NULL );
		void *o = sp_get(c2, NULL);
		st_object_is(o, i, 20);
		t( sp_destroy(o) == 0 );
		t( sp_destroy(c2) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c1) == 0 );
}

static void
cursor_consistency_rewrite2(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(st_r.env);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, v);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_phase();

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c1 = sp_cursor(db, o);
	v = 20;
	i = 0;
	while ((o = sp_get(c1, NULL))) {
		st_object_is(o, i, 15);
		t( sp_destroy(o) == 0 );

		tx = sp_begin(st_r.env);
		o = st_object(i, v);
		t( sp_set(tx, o) == 0 );
		t( sp_commit(tx) == 0 );
		i++;
	}
	t(i == 385);
	st_phase();

	t( sp_get(c0, NULL) == 0 );

	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c2 = sp_cursor(db, o);
	i = 0;
	while ((o = sp_get(c2, NULL))) {
		st_object_is(o, i, 20);
		t( sp_destroy(o) == 0 );
		i++;
	}
	t(i == 385);
	st_phase();

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );
}

static void
cursor_consistency_delete0(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(st_r.env);
	int i = 0;
	while (i < 385) {
		void *o = st_object(i, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_phase();

	tx = sp_begin(st_r.env);
	i = 0;
	while (i < 385) {
		void *o = st_object(i, i);
		t( sp_delete(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_phase();

	t( sp_get(c0, NULL) == NULL );

	t( sp_destroy(c0) == 0 );
}

static void
cursor_consistency_delete1(void)
{
	void *db = st_r.db;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c0 = sp_cursor(db, o);

	void *tx = sp_begin(st_r.env);
	int i = 0;
	while (i < 385) {
		o = st_object(i, i);
		t( sp_set(tx, o) == 0 );
		i++;
	}
	t( sp_commit(tx) == 0 );
	st_phase();

	tx = sp_begin(st_r.env);
	o = sp_object(db);
	t( o != NULL );
	t( sp_setstring(o, "order", ">=", 0) == 0 );
	void *c1 = sp_cursor(db, o);
	while ((o = sp_get(c1, NULL))) {
		t( sp_delete(tx, o) == 0 );
	}
	t( sp_commit(tx) == 0 );
	t( sp_destroy(c1) == 0 );
	st_phase();

	t( sp_get(c0, NULL) == NULL );
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
