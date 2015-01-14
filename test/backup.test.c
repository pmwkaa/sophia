
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
backup_test0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "backup.path", cx->suite->backupdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	void *o = sp_get(c, "backup.active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	t( sp_set(c, "backup.run") == 0 );

	/* state 0 */
	o = sp_get(c, "backup.active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	/* state 1 + 2 */
	t( sp_set(c, "scheduler.run") == 1 );

	/* state 3 */
	t( sp_set(c, "scheduler.run") == 0 );

	o = sp_get(c, "backup.active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	o = sp_get(c, "backup.last");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	o = sp_get(c, "backup.last_complete");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );

	/* recover backup */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	char path[1024];
	snprintf(path, sizeof(path), "%s/1", cx->suite->backupdir);
	t( sp_set(c, "sophia.path", path) == 0 );
	t( sp_set(c, "backup.path", cx->suite->backupdir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32") == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	/* ensure correct bsn recover */
	o = sp_get(c, "metric.seq_bsn");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	t( cur != NULL );
	i = 0;
	while ((o = sp_get(cur))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( i == 100 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
backup_test1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_set(c, "backup.path", cx->suite->backupdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "scheduler.checkpoint") == 0 );
	t( sp_set(c, "scheduler.run") == 1 );

	void *o = sp_get(c, "backup.active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	t( sp_set(c, "backup.run") == 0 );

	/* state 0 */
	o = sp_get(c, "backup.active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	/* state 1 + 2 */
	t( sp_set(c, "scheduler.run") == 1 );

	/* state 3 + branch */
	t( sp_set(c, "scheduler.run") == 0 );

	o = sp_get(c, "backup.active");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	o = sp_get(c, "backup.last");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	o = sp_get(c, "backup.last_complete");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );

	/* recover backup */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	char path[1024];
	snprintf(path, sizeof(path), "%s/1", cx->suite->backupdir);
	t( sp_set(c, "sophia.path", path) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.index.cmp", "u32") == 0 );
	t( sp_open(env) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );

	o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	t( cur != NULL );
	i = 0;
	while ((o = sp_get(cur))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( i == 100 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(env) == 0 );
}

stgroup *backup_group(void)
{
	stgroup *group = st_group("backup");
	st_groupadd(group, st_test("test_log_recover", backup_test0));
	st_groupadd(group, st_test("test_db_recover", backup_test1));
	return group;
}
