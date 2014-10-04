
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
object_copy0(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	void *copy = sp_copy(o);
	t( copy != NULL );
	int size = 0;
	t( *(int*)sp_get(copy, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_get(copy, "value", &size) == key );
	t( size == sizeof(key) );
	sp_destroy(o);
	sp_destroy(copy);
}

static void
object_copy1(stc *cx)
{
	void *db = cx->db;
	int key = 7;
	void *o = sp_object(db);
	t(o != NULL);
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(o, "value", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );

	void *c = sp_cursor(db, ">", NULL);
	o = sp_get(c);
	t( o != NULL );
	void *copy = sp_copy(o);
	t( copy != NULL );
	o = sp_get(c);
	t( o ==  NULL );
	sp_destroy(c);

	int size = 0;
	t( *(int*)sp_get(copy, "key", &size) == key );
	t( size == sizeof(key) );
	t( *(int*)sp_get(copy, "value", &size) == key );
	t( size == sizeof(key) );
	sp_destroy(copy);
}

stgroup *object_group(void)
{
	stgroup *group = st_group("object");
	st_groupadd(group, st_test("db", object_db));
	st_groupadd(group, st_test("setget", object_set_get));
	st_groupadd(group, st_test("copy0", object_copy0));
	st_groupadd(group, st_test("copy1", object_copy1));
	return group;
}
