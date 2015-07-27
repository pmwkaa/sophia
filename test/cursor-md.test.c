
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
cursor_md_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "t0", 0) == 0 );
	t( sp_setstring(env, "db", "t1", 0) == 0 );
	t( sp_setstring(env, "db.t0.index.key", "u32", 0) == 0 );
	t( sp_setstring(env, "db.t1.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.t0.sync", 0) == 0 );
	t( sp_setint(env, "db.t1.sync", 0) == 0 );

	void *t0 = sp_getobject(env, "db.t0");
	t( t0 != NULL );
	void *t1 = sp_getobject(env, "db.t1");
	t( t1 != NULL );
	t( sp_open(env) == 0 );

	uint32_t key = 7;
	uint32_t value = 7;
	void *o = sp_object(t0);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(t0, o) == 0 );
	o = sp_object(t1);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(t1, o) == 0 );

	void *c = sp_cursor(env);

	value = 8;
	o = sp_object(t0);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(t0, o) == 0 );
	o = sp_object(t1);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(t1, o) == 0 );

	o = sp_object(t0);
	o = sp_get(c, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "value", 0) == 7 );
	o = sp_get(c, o);
	t( o == NULL );

	o = sp_object(t1);
	o = sp_get(c, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "value", 0) == 7 );
	o = sp_get(c, o);
	t( o == NULL );

	sp_destroy(c);

	o = sp_object(t0);
	sp_setstring(o, "order", ">", 0);
	o = sp_get(t0, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "value", 0) == 8 );
	o = sp_get(t0, o);
	t( o == NULL );

	o = sp_object(t1);
	sp_setstring(o, "order", ">", 0);
	o = sp_get(t1, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "value", 0) == 8 );
	o = sp_get(t1, o);
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

stgroup *cursor_md_group(void)
{
	stgroup *group = st_group("cursor_md");
	st_groupadd(group, st_test("test0", cursor_md_test0));
	return group;
}
