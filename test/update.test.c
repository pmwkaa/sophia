
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libss.h>
#include <libst.h>
#include <sophia.h>

static void
update_no_operator(stc *cx ssunused)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	int i = 0;
	while ( i < 100 ) {
		void *o = sp_object(db);
		t( sp_set(o, "key", &i, sizeof(i)) == 0 );
		t( sp_set(o, "value", &i, sizeof(i)) == 0 );
		t( sp_update(db, o) == 0 );
		i++;
	}

	i = 0;
	void *o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	while ((o = sp_get(cur))) {
		t( *(int*)sp_get(o, "key", NULL) == i );
		t( *(int*)sp_get(o, "value", NULL) == i );
		t( sp_destroy(o) == 0 );
		i++;
	}
	sp_destroy(cur);

	t( sp_destroy(env) == 0 );
}

static int update_ops = 0;

static int
update_operator(void *src, int srcsize, void *update, int update_size, void *arg,
                void **result, int *resultsize)
{
	(void)arg;
	char *s = malloc(srcsize);
	assert(srcsize == update_size);
	memcpy(s, update, srcsize);
	*result = s;
	*resultsize = srcsize;
	update_ops++;
	return 0;
}

static void
update_set_update_get_index(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_get_branch0(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_get_branch1(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_get_compact(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_index(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_branch0(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_branch1(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_branch2(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_compact(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );
	t( sp_set(c, "db.test.compact") == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up1 );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_set_update_update_get_cursor(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up0 = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up0, sizeof(up0)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	int up1 = 778;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up1, sizeof(up1)) == 0 );
	t( sp_update(db, o) == 0 );

	t( sp_set(c, "db.test.branch") == 0 );

	o = sp_object(db);
	t( o != NULL );
	void *cur = sp_cursor(db, o);
	o = sp_get(cur);
	t( *(int*)sp_get(o, "value", NULL) == up1 );
	sp_destroy(o);
	o = sp_get(cur);
	t( o == NULL );
	sp_destroy(cur);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_sx_set_update_get(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *tx = sp_begin(env);

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(tx, o) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(tx, o) == 0 );

	t( sp_commit(tx) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 1 );

	t( sp_destroy(env) == 0 );
}

static void
update_sx_set_update_update_get(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *tx = sp_begin(env);

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(tx, o) == 0 );

	o = sp_object(db);
	int up = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(tx, o) == 0 );

	o = sp_object(db);
	up = 778;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(tx, o) == 0 );

	t( sp_commit(tx) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(db, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up );
	sp_destroy(o);

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

static void
update_sx_set_update_get_branch(stc *cx ssunused)
{
	update_ops = 0;

	char pointer[64];
	snprintf(pointer, sizeof(pointer), "pointer: %p", (void*)update_operator);

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.index.key", "u32", NULL) == 0 );
	t( sp_set(c, "db.test.index.update", pointer, NULL) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "compaction.0.branch_wm", "1") == 0 );
	t( sp_open(env) == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *o = sp_object(db);
	int i = 0;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &i, sizeof(i)) == 0 );
	t( sp_set(db, o) == 0 );

	void *tx = sp_begin(env);

	o = sp_object(db);
	int up = 777;
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	t( sp_set(o, "value", &up, sizeof(up)) == 0 );
	t( sp_update(tx, o) == 0 );

	o = sp_object(db);
	t( sp_set(o, "key", &i, sizeof(i)) == 0 );
	o = sp_get(tx, o);
	t( o != NULL );
	t( *(int*)sp_get(o, "value", NULL) == up );
	sp_destroy(o);

	t( sp_commit(tx) == 0 );

	t( update_ops == 2 );

	t( sp_destroy(env) == 0 );
}

stgroup *update_group(void)
{
	stgroup *group = st_group("update");
	st_groupadd(group, st_test("no_operator", update_no_operator));
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
	st_groupadd(group, st_test("sx_set_update_get", update_sx_set_update_get));
	st_groupadd(group, st_test("sx_set_update_get", update_sx_set_update_get));
	st_groupadd(group, st_test("sx_set_update_update_get", update_sx_set_update_update_get));
	st_groupadd(group, st_test("sx_set_update_get_branch", update_sx_set_update_get_branch));
	return group;
}
