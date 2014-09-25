
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
test_empty_gte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_empty_gt(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	void *c = sp_cursor(db, ">", NULL);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_empty_lte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	void *c = sp_cursor(db, "<=", NULL);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_empty_lt(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	void *c = sp_cursor(db, "<", NULL);
	t( c != NULL );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_gte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_gt(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_lte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, "<=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_lt(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, "<", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 15;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 79;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 80 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 90 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte5(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 0;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, ">", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 5;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 20;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<=", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 9;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 8;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 7;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 2;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	key = 20;
	void *pos = sp_object(db);
	t( pos != NULL );
	t( sp_set(pos, "key", &key, sizeof(key)) == 0 );
	void *c = sp_cursor(db, "<", pos);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		void *c = sp_cursor(db, ">=", o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i );
		t( *(int*)sp_get(v, "value", NULL) == i );
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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
	i = 0;
	while (i < (385 - 1)) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		void *c = sp_cursor(db, ">", o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i + 1);
		t( *(int*)sp_get(v, "value", NULL) == i + 1);
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		void *c = sp_cursor(db, "<=", o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i);
		t( *(int*)sp_get(v, "value", NULL) == i);
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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
	i = 1;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		void *c = sp_cursor(db, "<", o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == i - 1);
		t( *(int*)sp_get(v, "value", NULL) == i - 1);
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte_random(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	unsigned int seed = time(NULL);
	srand(seed);

	void *tx = sp_begin(db);
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

	srand(seed);
	i = 0;
	while (i < 270) {
		int key = rand();
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		void *c = sp_cursor(db, ">=", o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == key);
		t( *(int*)sp_get(v, "value", NULL) == i);
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte_random(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	unsigned int seed = time(NULL);
	srand(seed);

	void *tx = sp_begin(db);
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

	srand(seed);
	i = 0;
	while (i < 403) {
		int key = rand();
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		void *c = sp_cursor(db, "<=", o);
		t( c != NULL );
		void *v = sp_get(c);
		t( v != NULL );
		t( *(int*)sp_get(v, "key", NULL) == key);
		t( *(int*)sp_get(v, "value", NULL) == i);
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_consistency_0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );

	void *tx = sp_begin(db);
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

	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );

	tx = sp_begin(db);
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

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_5(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_6(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_7(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_8(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_9(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 0 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 5 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 6 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 7 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 8 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_get(c) == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_n(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );
	int rc;

	void *tx = sp_begin(db);
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

	void *c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	tx = sp_begin(db);
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

	tx = sp_begin(db);
	t( tx != NULL );
	k = 4;
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "key", &k, sizeof(k)) == 0 );
	t( sp_set(o, "value", &v, sizeof(v)) == 0 );
	t( sp_set(tx, o) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c2 = sp_cursor(db, ">=", NULL);
	t( c2 != NULL );

	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );
	o = sp_get(c);
	t( o == NULL );
	sp_destroy(c);

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 0 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 1 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 2 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 3 );
	t( *(int*)sp_get(o, "value",  NULL) == 2 );

	o = sp_get(c2);
	t( o != NULL );
	t( *(int*)sp_get(o, "key",  NULL) == 4 );
	t( *(int*)sp_get(o, "value",  NULL) == 3 );
	t( sp_get(c2) == NULL );
	t( sp_destroy(c2) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_rewrite0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	void *c0 = sp_cursor(db, ">=", NULL, 0);

	void *tx = sp_begin(db);
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
	sp_commit(tx);

	void *c1 = sp_cursor(db, ">=", NULL, 0);

	tx = sp_begin(db);
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
	sp_commit(tx);

	void *c2 = sp_cursor(db, ">=", NULL);

	t( sp_get(c0) == NULL );

	i = 0;
	while (sp_get(c1)) {
		void *o = sp_object(c1);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 15 );
		i++;
	}
	t(i == 385);

	i = 0;
	while (sp_get(c2)) {
		void *o = sp_object(c2);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 20 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_rewrite1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	void *c0 = sp_cursor(db, ">=", NULL, 0);

	void *tx = sp_begin(db);
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
	sp_commit(tx);

	void *c1 = sp_cursor(db, ">=", NULL);

	tx = sp_begin(db);
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
	sp_commit(tx);

	t( sp_get(c0) == NULL );

	i = 0;
	while (sp_get(c1)) {
		void *o = sp_object(c1);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 15 );
		i++;
	}
	t(i == 385);

	i = 0;
	while (i < 385) {
		void *ckey = sp_object(db);
		t( sp_set(ckey, "key", &i, sizeof(i)) == 0 );
		void *c2 = sp_cursor(db, ">=", ckey);
		t( c2 != NULL );
		void *o = sp_object(c2);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 20 );
		t( sp_destroy(c2) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c1) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_rewrite2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	void *c0 = sp_cursor(db, ">=", NULL, 0);

	void *tx = sp_begin(db);
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
	sp_commit(tx);

	void *c1 = sp_cursor(db, ">=", NULL);
	v = 20;
	i = 0;
	while (sp_get(c1)) {
		void *o = sp_object(c1);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 15 );

		tx = sp_begin(db);
		o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &v, sizeof(v)) == 0 );
		t( sp_set(tx, o) == 0 );
		t( sp_commit(tx) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_get(c0) == 0 );

	void *c2 = sp_cursor(db, ">=", NULL, 0);
	i = 0;
	while (sp_get(c2)) {
		void *o = sp_object(c2);
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == 20 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_delete0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *conf = sp_ctl(db, "conf");
	t( conf != NULL );
	t( sp_set(conf, "storage.logdir", "log") == 0 );
	t( sp_set(conf, "storage.dir", "test") == 0 );
	t( sp_set(conf, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(conf, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	void *c0 = sp_cursor(db, ">=", NULL);

	void *tx = sp_begin(db);
	int i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(tx, o) == 0 );
		i++;
	}
	sp_commit(tx);

	tx = sp_begin(db);
	i = 0;
	while (i < 385) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_delete(tx, o) == 0 );
		i++;
	}
	sp_commit(tx);

	t( sp_get(c0) == NULL );

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_empty_gte );
	test( test_empty_gt );
	test( test_empty_lte );
	test( test_empty_lt );
	test( test_gte );
	test( test_gt );
	test( test_lte );
	test( test_lt );
	test( test_pos_gte0 );
	test( test_pos_gte1 );
	test( test_pos_gte2 );
	test( test_pos_gte3 );
	test( test_pos_gte4 );
	test( test_pos_gte5 );
	test( test_pos_gt0 );
	test( test_pos_gt1 );
	test( test_pos_gt2 );
	test( test_pos_lte0 );
	test( test_pos_lte1 );
	test( test_pos_lte2 );
	test( test_pos_lte3 );
	test( test_pos_lte4 );
	test( test_pos_lt0 );
	test( test_pos_lt1 );
	test( test_pos_lt2 );
	test( test_pos_lt3 );
	test( test_pos_lt4 );
	test( test_pos_gte_range );
	test( test_pos_gt_range );
	test( test_pos_lte_range );
	test( test_pos_lt_range );
	test( test_pos_gte_random );
	test( test_pos_lte_random );
	test( test_consistency_0 );
	test( test_consistency_1 );
	test( test_consistency_2 );
	test( test_consistency_3 );
	test( test_consistency_4 );
	test( test_consistency_5 );
	test( test_consistency_6 );
	test( test_consistency_7 );
	test( test_consistency_8 );
	test( test_consistency_9 );
	test( test_consistency_n );
	test( test_consistency_rewrite0 );
	test( test_consistency_rewrite1 );
	test( test_consistency_rewrite2 );
	test( test_delete0 );
	return 0;
}
