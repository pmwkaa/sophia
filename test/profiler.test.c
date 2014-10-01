
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

st *profiler_group(void)
{
	st *group = st_def("profiler", NULL);
	return group;
}
