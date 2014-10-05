
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
profiler_count(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db.test.logdir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "db.test.threads", "0") == 0 );
	t( sp_set(c, "db.test.node_branch_wm", "0") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_get(c, "db.test.profiler.total_branch_count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.profiler.total_node_count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	o = sp_get(c, "db.test.profiler.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);
	t( sp_set(c, "db.test.run_branch") == 0 );

	o = sp_get(c, "db.test.profiler.total_branch_count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	o = sp_get(c, "db.test.profiler.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	i = 0;
	while ( i < 10 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	o = sp_get(c, "db.test.profiler.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "110") == 0 );
	sp_destroy(o);

	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == 0 );

	o = sp_get(c, "db.test.profiler.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

#if 0
static void
test_profiler(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.logdir", "log") == 0 );
	t( sp_set(c, "storage.dir", "test") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.node_branch_wm", 0) == 0 );
	t( sp_set(c, "storage.threads", 0) == 0 );
	t( sp_open(env) == 0 );

	void *profiler = sp_ctl(db, "profiler");
	t( profiler != NULL );
	t( *(uint32_t*)sp_get(profiler, "total_node_count", NULL) == 1 );
	t( *(uint32_t*)sp_get(profiler, "total_branch_count", NULL) == 0 );
	t( *(uint64_t*)sp_get(profiler, "count", NULL) == 0 );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( *(uint64_t*)sp_get(profiler, "count", NULL) == 100 );
	t( sp_set(sp_ctl(db, "ctl"), "branch") == 0 );

	t( *(uint32_t*)sp_get(profiler, "total_branch_count", NULL) == 1 );
	t( *(uint64_t*)sp_get(profiler, "count", NULL) == 100 );

	i = 0;
	while ( i < 10 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( *(uint64_t*)sp_get(profiler, "count", NULL) == 100 + 10);
	t( sp_set(sp_ctl(db, "ctl"), "branch") == 0 );
	t( sp_set(sp_ctl(db, "ctl"), "merge") == 0 );

	t( *(uint64_t*)sp_get(profiler, "count", NULL) == 100);

	t( sp_destroy(env) == 0 );
}
#endif

stgroup *profiler_group(void)
{
	stgroup *group = st_group("profiler");
	st_groupadd(group, st_test("count", profiler_count));
	return group;
}
