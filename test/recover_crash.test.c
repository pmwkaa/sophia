
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
recovercrash_branch0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	int key = 7;
	void *o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(c, "db.test.error_injection.si_branch_0", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db.incomplete") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db.incomplete") == 0 );
}

static void
recovercrash_branch1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	int key = 7;
	void *o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(c, "db.test.error_injection.si_branch_1", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
}

static void
recovercrash_merge0(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	int key = 7;
	void *o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(c, "db.test.error_injection.si_merge_0", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
}

static void
recovercrash_merge1(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	int key = 7;
	void *o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(c, "db.test.error_injection.si_merge_1", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000002.db") == 1 );
}

static void
recovercrash_merge2(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );
	int key = 7;
	void *o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 8;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	key = 9;
	o = sp_object(db);
	t( o != 0 );
	t( sp_set(o, "key", &key, sizeof(key)) == 0 );
	t( sp_set(db, o) == 0 );
	t( sp_set(c, "db.test.error_injection.si_merge_2", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 7 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 8 );
	o = sp_get(c);
	t( o != NULL );
	t( *(int*)sp_get(o, "key", NULL) == 9 );
	o = sp_get(c);
	t( o == NULL );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000002.db") == 1 );
}

static void
recovercrash_merge3(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "scheduler.node_size", "8") == 0 );
	t( sp_set(c, "scheduler.node_page_size", "31") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 10) {
		void *o = sp_object(db);
		t( o != 0 );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.error_injection.si_merge_0", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	i = 0;
	void *o;
	while ((o = sp_get(c))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 0 );
}

static void
recovercrash_merge4(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "scheduler.node_size", "8") == 0 );
	t( sp_set(c, "scheduler.node_page_size", "31") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 10) {
		void *o = sp_object(db);
		t( o != 0 );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.error_injection.si_merge_1", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "8") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	i = 0;
	void *o;
	while ((o = sp_get(c))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000002.db") == 1 );
	t( exists(cx->suite->dir, "0000000003.db") == 1 );
}

static void
recovercrash_merge5(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "scheduler.node_size", "8") == 0 );
	t( sp_set(c, "scheduler.node_page_size", "31") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 10) {
		void *o = sp_object(db);
		t( o != 0 );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.error_injection.si_merge_2", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	i = 0;
	void *o;
	while ((o = sp_get(c))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000002.db") == 1 );
	t( exists(cx->suite->dir, "0000000003.db") == 1 );
}

static void
recovercrash_merge6(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "scheduler.node_size", "8") == 0 );
	t( sp_set(c, "scheduler.node_page_size", "31") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 10) {
		void *o = sp_object(db);
		t( o != 0 );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.error_injection.si_merge_3", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 0 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	i = 0;
	void *o;
	while ((o = sp_get(c))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 1 );
	t( exists(cx->suite->dir, "0000000001.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000002.db") == 0 );
	t( exists(cx->suite->dir, "0000000003.db") == 0 );
}

static void
recovercrash_merge7(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "scheduler.node_size", "8") == 0 );
	t( sp_set(c, "scheduler.node_page_size", "31") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	int i = 0;
	while (i < 10) {
		void *o = sp_object(db);
		t( o != 0 );
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(db, o) == 0 );
		i++;
	}
	t( sp_set(c, "db.test.error_injection.si_merge_4", "1") == 0 );
	t( sp_set(c, "db.test.run_branch") == 0 );
	t( sp_set(c, "db.test.run_merge") == -1 );
	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000002.db") == 1 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 1 );

	/* recover */
	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "scheduler.node_branch_wm", "0") == 0 );
	t( sp_set(c, "db.test.log_dir", cx->suite->logdir) == 0 );
	t( sp_set(c, "db.test.log_sync", "0") == 0 );
	t( sp_set(c, "db.test.log_rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.dir", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.dir_sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	c = sp_cursor(db, ">=", NULL);
	t( c != NULL );
	i = 0;
	void *o;
	while ((o = sp_get(c))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		i++;
	}
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );

	t( exists(cx->suite->dir, "0000000000.db") == 0 );
	t( exists(cx->suite->dir, "0000000001.db") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000002.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.incomplete") == 0 );
	t( exists(cx->suite->dir, "0000000000.0000000003.db.seal") == 0 );
	t( exists(cx->suite->dir, "0000000002.db") == 1 );
	t( exists(cx->suite->dir, "0000000003.db") == 1 );
}

stgroup *recovercrash_group(void)
{
	stgroup *group = st_group("recover_crash");
	st_groupadd(group, st_test("branch0", recovercrash_branch0));
	st_groupadd(group, st_test("branch1", recovercrash_branch1));
	st_groupadd(group, st_test("merge0", recovercrash_merge0));
	st_groupadd(group, st_test("merge1", recovercrash_merge1));
	st_groupadd(group, st_test("merge2", recovercrash_merge2));
	st_groupadd(group, st_test("merge3", recovercrash_merge3));
	st_groupadd(group, st_test("merge4", recovercrash_merge4));
	st_groupadd(group, st_test("merge5", recovercrash_merge5));
	st_groupadd(group, st_test("merge6", recovercrash_merge6));
	st_groupadd(group, st_test("merge7", recovercrash_merge7));
	return group;
}
