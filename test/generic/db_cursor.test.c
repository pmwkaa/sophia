
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
db_cursor_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *snapcur = sp_getobject(env, "db");
	t( snapcur != NULL );

	t( sp_drop(db) == 0 );
	t( sp_destroy(db) == 0 ); /* unref */
	t( sp_destroy(db) == 0 ); /* shutdown */

	t( sp_setstring(env, "db", "test2", 0) == 0 );
	void *db2 = sp_getobject(env, "db.test2");
	t( db2 != NULL );
	t( sp_open(db2) == 0 );

	void *o;
	while ((o = sp_get(snapcur, NULL))) {
		t( o == db );
	}
	sp_destroy(snapcur);

	snapcur = sp_getobject(env, "db");
	t( snapcur != NULL );
	while ((o = sp_get(snapcur, NULL))) {
		t( o == db2 );
	}
	sp_destroy(snapcur);

	t( sp_destroy(env) == 0 );
}

stgroup *db_cursor_group(void)
{
	stgroup *group = st_group("db_cursor");
	st_groupadd(group, st_test("test0", db_cursor_test0));
	return group;
}
