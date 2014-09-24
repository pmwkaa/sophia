
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <sophia.h>
#include "suite.h"

static void
test_rollback(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
	t( tx != NULL );
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
	t( tx != NULL );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_get_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_commit_get0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_commit_get1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_rollback(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_rollback_get0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_rollback_get1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_get_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_commit_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value );
	sp_destroy(o);
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_rollback_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o == NULL );
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_get_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_get_commit_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o == NULL );
	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_set_commit_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == value );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_commit_get_set(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_get_commit(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit_get0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

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

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit_get1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

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

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit_get2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

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
	int rc = sp_commit(a);
	t( rc == 0 );

	int value_a = 10;
	int key_a = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

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

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_rollback_get0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );

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

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_rollback_get1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	int value_b = 15;
	int key_b = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_b, sizeof(key_b)) == 0 );
	t( sp_set(o, "value", &value_b, sizeof(value_b)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );

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

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_rollback_get2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

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
	int rc = sp_rollback(b);
	t( rc == 0 );

	int value_a = 10;
	int key_a = 7;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key_a, sizeof(key_a)) == 0 );
	t( sp_set(o, "value", &value_a, sizeof(value_a)) == 0 );
	t( sp_set(a, o) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );

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

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_a0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );

	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_a1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_b0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_b1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_rollback(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_ab0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(b, o) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_ab1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_rollback(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_a0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_a1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_b0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_b1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_a0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_a1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_b0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_rollback(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_b1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_rollback(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_n0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_n1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(b);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(b);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(c);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(c);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(c);
	t( rc == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 10 );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	void *tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 15 );
	sp_destroy(o);
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	t( sp_rollback(d) == 0 );
	t( sp_rollback(c) == 0 );
	t( sp_rollback(b) == 0 );
	t( sp_rollback(a) == 0 );
	t( sp_rollback(e) == 0 );
	t( sp_rollback(z) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

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

	/* 2 */
	t( sp_rollback(c) == 0 );

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

	/* 4 */
	t( sp_rollback(e) == 0 );

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

	/* 6 */
	t( sp_rollback(a) == 0 );
	t( sp_rollback(z) == 0 );

	tx = sp_begin(db);
	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 4 );
	sp_destroy(o);
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_sc_set_wait(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
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

	rc = sp_commit(tx);
	t( rc == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
test_sc_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	int key = 7;
	int v = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

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

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 8 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
test_s_set(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_s_set_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == key );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
test_s_set_delete_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

static void
test_s_set_delete_set_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	int key = 7;
	void *o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_delete(db, o) == 0 );

	int v = 8;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == 8 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_rollback );
	test( test_commit );
	test( test_set_commit );
	test( test_set_get_commit );
	test( test_set_commit_get0 );
	test( test_set_commit_get1 );
	test( test_set_rollback );
	test( test_set_rollback_get0 );
	test( test_set_rollback_get1 );
	test( test_set_set_commit );
	test( test_set_set_get_commit );
	test( test_set_set_commit_get );
	test( test_set_set_rollback_get );
	test( test_set_delete_get_commit );
	test( test_set_delete_get_commit_get );
	test( test_set_delete_set_commit_get );
	test( test_set_delete_commit_get_set );
	test( test_set_set_get_commit );
	test( test_p_set_commit );
	test( test_p_set_get_commit );
	test( test_p_set_commit_get0 );
	test( test_p_set_commit_get1 );
	test( test_p_set_commit_get2 );
	test( test_p_set_rollback_get0 );
	test( test_p_set_rollback_get1 );
	test( test_p_set_rollback_get2 );
	test( test_c_set_commit0 );
	test( test_c_set_commit1 );
	test( test_c_set_commit2 );
	test( test_c_set_commit_rollback_a0 );
	test( test_c_set_commit_rollback_a1 );
	test( test_c_set_commit_rollback_b0 );
	test( test_c_set_commit_rollback_b1 );
	test( test_c_set_commit_rollback_ab0 );
	test( test_c_set_commit_rollback_ab1 );
	test( test_c_set_commit_wait_a0 );
	test( test_c_set_commit_wait_a1 );
	test( test_c_set_commit_wait_b0 );
	test( test_c_set_commit_wait_b1 );
	test( test_c_set_commit_wait_rollback_a0 );
	test( test_c_set_commit_wait_rollback_a1 );
	test( test_c_set_commit_wait_rollback_b0 );
	test( test_c_set_commit_wait_rollback_b1 );
	test( test_c_set_commit_wait_n0 );
	test( test_c_set_commit_wait_n1 );
	test( test_c_set_commit_wait_rollback_n0 );
	test( test_c_set_commit_wait_rollback_n1 );
	test( test_c_set_commit_wait_rollback_n2 );
	test( test_c_set_commit_wait_rollback_n3 );
	test( test_c_set_commit_wait_rollback_n4 );
	test( test_c_set_get0 );
	test( test_c_set_get1 );
	test( test_c_set_get2 );
	test( test_c_set_get3 );
	test( test_sc_set_wait );
	test( test_sc_get );
	test( test_s_set );
	test( test_s_set_get );
	test( test_s_set_delete_get );
	test( test_s_set_delete_set_get );
	return 0;
}
