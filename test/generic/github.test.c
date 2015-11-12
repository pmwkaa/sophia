
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
github_97(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.index.key", "u32", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	/* we must pass sizeof(uint32_t) in sp_setstring() */
	uint32_t i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );   /* < */
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 ); /* < */
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *cur = sp_cursor(env);
	t( cur != NULL );

	void *o = sp_document(db);
	t( o != NULL );
	uint32_t key = 99;
	t( sp_setstring(o, "key", &key, sizeof(key)) == 0 ); /* <  */

	i = 0;
	while ((o = sp_get(cur, o)))
		i++;
	t( i == 1 );
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

stgroup *github_group(void)
{
	stgroup *group = st_group("github");
	st_groupadd(group, st_test("ticket_97", github_97));
	return group;
}
