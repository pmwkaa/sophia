
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
update_no_operator(void)
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
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
		t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
		t( sp_update(db, o) == 0 );
		i++;
	}

	i = 0;
	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	while ((o = sp_get(cur, o))) {
		t( *(int*)sp_getstring(o, "key", NULL) == i );
		t( *(int*)sp_getstring(o, "value", NULL) == i );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static int update_ops = 0;

static int
update_operator_orphan(int a_flags, void *a, int a_size,
                       int b_flags, void *b, int b_size, void *arg,
                       void **result, int *result_size)
{
	assert(a == NULL);
	assert(a_flags == 0);
	assert(a_size == 0);
	assert(b_flags == SVUPDATE);
	assert(b != NULL);
	(void)arg;
	char *c = malloc(b_size);
	memcpy(c, b, b_size);
	*result = c;
	*result_size = b_size;
	update_ops++;
	return 0;
}

static int
update_operator(int a_flags, void *a, int a_size,
                int b_flags, void *b, int b_size, void *arg,
                void **result, int *result_size)
{
	assert(a != NULL);
	assert(a_flags == 0); /* SET */
	assert(b_flags == SVUPDATE);
	assert(b != NULL);
	(void)arg;
	char *c = malloc(b_size);
	memcpy(c, b, b_size);
	*result = c;
	*result_size = b_size;
	update_ops++;
	return 0;
}

static void
update_update_get_index(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_orphan, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int up = 777;
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_update_get_branch0(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_orphan, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int up = 777;
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( update_ops == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_update_get_compact(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_orphan, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int up = 777;
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	t( update_ops == 1 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_get_index(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_get_branch0(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_get_branch1(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_get_compact(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_index(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_branch0(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_branch1(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_branch2(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_compact(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_cursor(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static int
update_operator_delete(int a_flags, void *a, int a_size,
                       int b_flags, void *b, int b_size, void *arg,
                       void **result, int *result_size)
{
	assert(a == NULL);
	assert(a_flags == 0);
	assert(b_flags == SVUPDATE);
	assert(b != NULL);
	(void)arg;
	char *c = malloc(b_size);
	memcpy(c, b, b_size);
	*result = c;
	*result_size = b_size;
	update_ops++;
	return 0;
}

static void
update_set_delete_update_get_index(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_delete_update_get_branch0(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_delete_update_get_branch1(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_delete_update_get_compact(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_delete_update_get_index(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_delete_update_get_branch0(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_delete_update_get_branch1(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_delete_update_get_compact(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_delete, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_getstring(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_sx_set_update_get(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *tx = sp_begin(env);

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(tx, o) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(tx, o) == -1 );

	t( sp_commit(tx) == 0 );

	t( update_ops == 0 );

	t( sp_destroy(env) == 0 );
}

static void
update_sx_update_update(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *tx = sp_begin(env);

	void *o;
	int i = 0;

	o = sp_object(db);
	int up = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(tx, o) == 0 );

	o = sp_object(db);
	up = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(tx, o) == -1 );

	t( sp_commit(tx) == 0 );

	t( update_ops == 0 );

	t( sp_destroy(env) == 0 );
}

static void
update_cursor0(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(env);
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_cursor1(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	void *cur = sp_cursor(env);

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == up0 );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_cursor2(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	void *cur = sp_cursor(env);

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( o != NULL );
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == i );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 0 );

	t( sp_destroy(env) == 0 );
}

static void
update_cursor3(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator_orphan, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	void *cur = sp_cursor(env);
	o = sp_object(db);
	t( o != NULL );
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == up1 );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static int
update_operator2(int a_flags, void *a, int a_size,
                int b_flags, void *b, int b_size, void *arg,
                void **result, int *result_size)
{
	assert(b_flags == SVUPDATE);
	assert(b != NULL);
	(void)arg;
	char *c = malloc(b_size);
	memcpy(c, b, b_size);
	*result = c;
	*result_size = b_size;
	update_ops++;
	return 0;
}

static void
update_cursor4(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator2, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	void *cur = sp_cursor(env);
	o = sp_object(db);
	t( o != NULL );
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == up0 );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_cursor5(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator2, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_delete(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	void *cur = sp_cursor(env);
	o = sp_object(db);
	t( o != NULL );
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == up0 );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_cursor6(void)
{
	update_ops = 0;

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
	t( sp_setstring(env, "db.test.index.update", update_operator2, 0) == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_setstring(o, "key", &i, sizeof(i)) == 0 );
	t( sp_setstring(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_setint(env, "db.test.branch", 0) == 0 );
	t( sp_setint(env, "db.test.compact", 0) == 0 );

	void *cur = sp_cursor(env);
	o = sp_object(db);
	t( o != NULL );
	o = sp_get(cur, o);
	t( *(int*)sp_getstring(o, "value", NULL) == up0 );
	o = sp_get(cur, o);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

stgroup *update_group(void)
{
	stgroup *group = st_group("update");
	st_groupadd(group, st_test("no_operator", update_no_operator));
	st_groupadd(group, st_test("update_get_index", update_update_get_index));
	st_groupadd(group, st_test("update_get_branch0", update_update_get_branch0));
	st_groupadd(group, st_test("update_get_compact", update_update_get_compact));
	st_groupadd(group, st_test("set_update_get_index", update_set_update_get_index));
	st_groupadd(group, st_test("set_update_get_branch0", update_set_update_get_branch0));
	st_groupadd(group, st_test("set_update_get_branch1", update_set_update_get_branch1));
	st_groupadd(group, st_test("set_update_get_compact", update_set_update_get_compact));
	st_groupadd(group, st_test("set_update_update_get_index", update_set_update_update_get_index));
	st_groupadd(group, st_test("set_update_update_get_branch0", update_set_update_update_get_branch0));
	st_groupadd(group, st_test("set_update_update_get_branch1", update_set_update_update_get_branch1));
	st_groupadd(group, st_test("set_update_update_get_branch2", update_set_update_update_get_branch2));
	st_groupadd(group, st_test("set_update_update_get_compact", update_set_update_update_get_compact));
	st_groupadd(group, st_test("set_update_update_get_cursor", update_set_update_update_get_cursor));
	st_groupadd(group, st_test("set_delete_update_get_index", update_set_delete_update_get_index));
	st_groupadd(group, st_test("set_delete_update_get_branch0", update_set_delete_update_get_branch0));
	st_groupadd(group, st_test("set_delete_update_get_branch1", update_set_delete_update_get_branch1));
	st_groupadd(group, st_test("set_delete_update_get_compact", update_set_delete_update_get_compact));
	st_groupadd(group, st_test("delete_update_get_index", update_delete_update_get_index));
	st_groupadd(group, st_test("delete_update_get_branch0", update_delete_update_get_branch0));
	st_groupadd(group, st_test("delete_update_get_branch1", update_delete_update_get_branch1));
	st_groupadd(group, st_test("delete_update_get_compact", update_delete_update_get_compact));
	st_groupadd(group, st_test("sx_set_update_get", update_sx_set_update_get));
	st_groupadd(group, st_test("sx_update_update", update_sx_update_update));
	st_groupadd(group, st_test("update_cursor0", update_cursor0));
	st_groupadd(group, st_test("update_cursor1", update_cursor1));
	st_groupadd(group, st_test("update_cursor2", update_cursor2));
	st_groupadd(group, st_test("update_cursor3", update_cursor3));
	st_groupadd(group, st_test("update_cursor4", update_cursor4));
	st_groupadd(group, st_test("update_cursor5", update_cursor5));
	st_groupadd(group, st_test("update_cursor6", update_cursor6));
	return group;
}
