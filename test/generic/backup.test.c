
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
backup_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "backup.path", st_r.conf->backup_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_getint(env, "backup.active") == 0 );
	t( sp_setint(env, "backup.run", 0) == 0 );

	/* state 0 */
	t( sp_getint(env, "backup.active") == 1 );

	/* state 1 + 2 */
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	/* scheme backup completion */
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	/* state 3 */
	t( sp_setint(env, "scheduler.run", 0) == 0 );

	t( sp_getint(env, "backup.active") == 0 );
	t( sp_getint(env, "backup.last") == 1 );
	t( sp_getint(env, "backup.last_complete") == 1 );

	t( sp_destroy(env) == 0 );

	/* recover backup */
	char path[1024];
	snprintf(path, sizeof(path), "%s/1", st_r.conf->backup_dir);

	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", path, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "backup.path", st_r.conf->backup_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	/* ensure correct bsn recover */
	t( sp_getint(env, "metric.bsn") == 1 );

	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	t( cur != NULL );
	i = 0;
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		i++;
	}
	t( i == 100 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
backup_test1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "backup.path", st_r.conf->backup_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_document(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "scheduler.checkpoint", 0) == 0 );
	t( sp_setint(env, "scheduler.run", 0) == 1 );

	t( sp_getint(env, "backup.active") == 0 );
	t( sp_setint(env, "backup.run", 0) == 0 );

	/* state 0 */
	t( sp_getint(env, "backup.active") == 1 );

#if 0
	/* state 1 + 2 */
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	/* scheme backup completion */
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	/* state 3 + branch */
	t( sp_setint(env, "scheduler.run", 0) == 1 );
	t( sp_setint(env, "scheduler.run", 0) == 0 );
#endif

	int rc;
	while ( (rc = sp_setint(env, "scheduler.run", 0)) > 0 );
	t( rc == 0 );

	t( sp_getint(env, "backup.active") == 0 );
	t( sp_getint(env, "backup.last") == 1 );
	t( sp_getint(env, "backup.last_complete") == 1 );

	t( sp_destroy(env) == 0 );

	/* recover backup */
	char path[1024];
	snprintf(path, sizeof(path), "%s/1", st_r.conf->backup_dir);
	env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", path, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setstring(env, "backup.path", st_r.conf->backup_dir, 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	void *o = sp_document(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	t( cur != NULL );
	i = 0;
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
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
