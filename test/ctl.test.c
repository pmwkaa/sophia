
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
ctl_version(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );

	void *c = sp_ctl(env);
	t( c != NULL );

	void *o = sp_get(c, "sophia.version");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1.2.2") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
ctl_error_injection(stc *cx srunused)
{
	void *env = sp_env();
	t( env != NULL );

	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "db", "test") == 0 );

	void *o = sp_get(c, "debug.error_injection.si_branch_0");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "0") == 0 );
	sp_destroy(o);

	t( sp_set(c, "debug.error_injection.si_branch_0", "1") == 0 );
	o = sp_get(c, "debug.error_injection.si_branch_0");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
ctl_scheduler(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );

	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "2") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );

	void *o = sp_get(c, "scheduler.0.trace");
	t( o == NULL );
	t( sp_open(env) == 0 );

	o = sp_get(c, "scheduler.0.trace");
	t( o != NULL );
	char *v = sp_get(o, "value", NULL);
	t( strcmp(v, "malfunction") != 0 );
	sp_destroy(o);

	o = sp_get(c, "scheduler.1.trace");
	t( o != NULL );
	v = sp_get(o, "value", NULL);
	t( strcmp(v, "malfunction") != 0 );
	sp_destroy(o);

	o = sp_get(c, "scheduler.2.trace");
	t( o == NULL );

	t( sp_destroy(env) == 0 );
}

static void
ctl_compaction(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );

	char path[64];
	snprintf(path, sizeof(path), "compaction.60");
	void *o = sp_get(c, path);
	t( o == NULL );

	t( sp_set(c, "compaction", "58") == 0 );

	snprintf(path, sizeof(path), "compaction.50.mode");
	o = sp_get(c, path);
	t( o != NULL );
	sp_destroy(o);

	int i = 10;
	while (i < 100) {
		snprintf(path, sizeof(path), "%d", i);
		t( sp_set(c, "compaction", path) == 0 );
		i += 10;
	}
	i = 10;
	while (i < 100) {
		snprintf(path, sizeof(path), "compaction.%d.branch_wm", i);
		o = sp_get(c, path);
		t( o != NULL );
		sp_destroy(o);
		i += 10;
	}
	t( sp_destroy(env) == 0 );
}

static void
ctl_validation(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "log.enable", "0") == 0 );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_open(env) == 0 );

	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == -1 );
	t( sp_set(c, "memory.limit", "0") == -1 );
	t( sp_set(c, "compaction.page_size", "0") == -1 );
	t( sp_set(c, "compaction.node_size", "0") == -1 );
	t( sp_set(c, "scheduler.threads", "0") == -1 );

	t( sp_set(c, "log.enable", "0") == -1 );
	t( sp_set(c, "log.path", "path") == -1 );
	t( sp_set(c, "log.sync", "0") == -1 );
	t( sp_set(c, "log.rotate_wm", "0") == -1 );
	t( sp_set(c, "log.rotate_sync", "0") == -1 );
	t( sp_set(c, "log.two_phase_commit", "0") == -1 );
	t( sp_set(c, "log.commit_lsn", "0") == -1 );

	t( sp_set(c, "db", "test") == 0 );
	void *db = sp_get(c, "db.test");
	t( db != NULL );
	t( sp_open(db) == 0 );
	t( sp_set(c, "db.test.path", "path") == -1 );
	t( sp_set(c, "db.test.index.cmp", NULL, NULL) == -1 );

	t( sp_destroy(env) == 0 );
}

static void
ctl_db(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_open(env) == 0 );

	void *db = sp_get(c, "db.test");
	t( db != NULL );

	void *dbctl = sp_ctl(db);
	t( dbctl != NULL );

	void *o = sp_get(dbctl, "name");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "test") == 0 );
	sp_destroy(o);

	o = sp_get(dbctl, "id");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

static void
ctl_cursor(stc *cx)
{
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_open(env) == 0 );
	t( sp_set(c, "snapshot", "test_snapshot0") == 0 );
	void *o;
	void *cur = sp_cursor(c);
	t( cur != NULL );
	printf("\n");
	while ((o = sp_get(cur))) {
		char *key = sp_get(o, "key", NULL);
		char *value = sp_get(o, "value", NULL);
		printf("%s", key);
		if (value)
			printf(" = %s\n", value);
		else
			printf(" = \n");
	}
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(env) == 0 );
}

stgroup *ctl_group(void)
{
	stgroup *group = st_group("ctl");
	st_groupadd(group, st_test("version", ctl_version));
	st_groupadd(group, st_test("error_injection", ctl_error_injection));
	st_groupadd(group, st_test("scheduler", ctl_scheduler));
	st_groupadd(group, st_test("compaction", ctl_compaction));
	st_groupadd(group, st_test("validation", ctl_validation));
	st_groupadd(group, st_test("db", ctl_db));
	st_groupadd(group, st_test("cursor", ctl_cursor));
	return group;
}
