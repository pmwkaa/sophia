
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
lru_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );
	t( sp_setint(env, "compaction.0.lru_prio", 1) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "db.test.lru", 100 * 1024) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	char value[1024];
	memset(value, 0, sizeof(value));
	int i = 0;
	while ( i < 200 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	int64_t size = sp_getint(env, "db.test.index.size");

	t( sp_setint(env, "scheduler.lru", 1) == 0 );
	t( sp_getint(env, "scheduler.lru_active") == 1 );

	int rc;
	while ( (rc = sp_setint(env, "scheduler.run", 0)) > 0 );
	t( rc == 0 );

	t( sp_getint(env, "scheduler.lru_active") == 0 );

	int64_t size_after = sp_getint(env, "db.test.index.size");
	t( size_after < size );

	t( sp_destroy(env) == 0 );
}

stgroup *lru_group(void)
{
	stgroup *group = st_group("lru");
	st_groupadd(group, st_test("test0", lru_test0));
	return group;
}
