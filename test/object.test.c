
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
object_db(stc *cx)
{
	void *db = cx->db;
	void *o = sp_object(db);
	t(o != NULL);
	sp_destroy(o);
	o = sp_object(cx->env);
	t(o == NULL);
}

static void
object_set_get(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	int size = 0;
	t( *(int*)sp_get(o, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_get(o, "value", &size) == key );
	t( size == sizeof(key) );
	sp_destroy(o);
}

static void
object_lsn0(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_get(o, "lsn") == NULL );
	t( sp_set(db, o) == 0 );
	o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "order", ">") == 0 );
	void *c = sp_cursor(db, o);
	o = sp_get(c);
	t( o != NULL );
	int size = 0;
	t( *(int*)sp_get(o, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_get(o, "value", &size) == key );
	t( size == sizeof(key) );
	t( *(uint64_t*)sp_get(o, "lsn") > 0 );
	o = sp_get(c);
	t( o ==  NULL );
	sp_destroy(c);
}

static void
object_readonly0(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	o = sp_get(db, o);
	t( o!= NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == -1 );
	t( *(int*)sp_get(o, "key", NULL) == key );
	sp_destroy(o);
}

static void
object_readonly1(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	o = sp_object(db);
	t( o != NULL );
	t( sp_set(o, "order", ">") == 0 );
	void *c = sp_cursor(db, o);
	o = sp_get(c);
	t( o != NULL );
	t( sp_set(o, "key", &key, sizeof(key)) == -1 );
	sp_destroy(c);
}

stgroup *object_group(void)
{
	stgroup *group = st_group("object");
	st_groupadd(group, st_test("db", object_db));
	st_groupadd(group, st_test("setget", object_set_get));
	st_groupadd(group, st_test("lsn0", object_lsn0));
	st_groupadd(group, st_test("readonly0", object_readonly0));
	st_groupadd(group, st_test("readonly1", object_readonly1));
	return group;
}
