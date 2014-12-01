
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
checkpoint_test0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int key = 0;
	while (key < 10) {
		void *o = sp_object(db);
		t( o != NULL );
		t( sp_set(o, "key", &key, sizeof(key)) == 0 );
		t( sp_set(o, "value", &key, sizeof(key)) == 0 );
		t( sp_set(db, o) == 0 );
		key++;
	}

	void *o = sp_get(c, "scheduler.checkpoint_active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);
	o = sp_get(c, "scheduler.checkpoint_lsn");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);
	o = sp_get(c, "scheduler.checkpoint_lsn_last");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	t( sp_set(c, "scheduler.checkpoint") == 0 );

	o = sp_get(c, "scheduler.checkpoint_active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);
	o = sp_get(c, "scheduler.checkpoint_lsn");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "11") == 0 );
	sp_destroy(o);
	o = sp_get(c, "scheduler.checkpoint_lsn_last");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	t( sp_set(c, "scheduler.run") == 1 );
	t( sp_set(c, "scheduler.run") == 0 );

	o = sp_get(c, "scheduler.checkpoint_active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);
	o = sp_get(c, "scheduler.checkpoint_lsn");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);
	o = sp_get(c, "scheduler.checkpoint_lsn_last");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "11") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *checkpoint_group(void)
{
	stgroup *group = st_group("checkpoint");
	st_groupadd(group, st_test("test0", checkpoint_test0));
	return group;
}
