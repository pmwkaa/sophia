
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
cursor_read_commited(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *c = sp_cursor(env);
	t( sp_setint(c, "read_commited", 1) == 0 );

	t( sp_setint(c, "read_commited", 1) == -1 );
	t( sp_setint(c, "read_commited", 0) == -1 );

	uint32_t key = 7;
	uint32_t value = 7;
	void *o = sp_document(db);
	t( o != NULL );
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 );
	t( sp_setstring(o, "value", &value, sizeof(value)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_document(db);
	o = sp_get(c, o);
	t( o != NULL );
	t( *(uint32_t*)sp_getstring(o, "value", 0) == 7 );
	o = sp_get(c, o);
	t( o == NULL );

	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

stgroup *cursor_rc_group(void)
{
	stgroup *group = st_group("cursor_rc");
	st_groupadd(group, st_test("read_commited0", cursor_read_commited));
	return group;
}
