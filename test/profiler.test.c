
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
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );

	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_get(c, "db.test.index.branch_count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);
	o = sp_get(c, "db.test.index.node_count");
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
	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);
	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_get(c, "db.test.index.branch_count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "2") == 0 );
	sp_destroy(o);

	o = sp_get(c, "db.test.index.count");
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

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "110") == 0 );
	sp_destroy(o);

	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *profiler_group(void)
{
	stgroup *group = st_group("profiler");
	st_groupadd(group, st_test("count", profiler_count));
	return group;
}
