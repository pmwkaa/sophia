
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
secondary_index_test0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	/* unique */
	t( sp_setstring(env, "db", "primary", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.a", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.b", "u32", 0) == 0 );
	t( sp_setint(env, "db.primary.sync", 0) == 0 );

	/* non-unique */
	t( sp_setstring(env, "db", "secondary", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.a", "u32,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.b", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.secondary.sync", 0) == 0 );

	void *primary = sp_getobject(env, "db.primary");
	void *secondary = sp_getobject(env, "db.secondary");

	t( primary != NULL );
	t( secondary != NULL );

	t( sp_open(env) == 0 );

	void *tx = sp_begin(env);
	uint32_t a = 0;
	uint32_t b = 0;
	void *po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	void *so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	t( sp_getint(env, "performance.documents") == 1 );

	t( sp_setint(env, "db.primary.branch", 0) == 0 );
	t( sp_setint(env, "db.secondary.branch", 0) == 0 );

	sp_destroy(env);
}

static void
secondary_index_test_unique0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	/* unique */
	t( sp_setstring(env, "db", "primary", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.a", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.b", "u32", 0) == 0 );
	t( sp_setint(env, "db.primary.sync", 0) == 0 );

	/* unique */
	t( sp_setstring(env, "db", "secondary", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.a", "u32", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.b", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.secondary.sync", 0) == 0 );

	void *primary = sp_getobject(env, "db.primary");
	void *secondary = sp_getobject(env, "db.secondary");

	t( primary != NULL );
	t( secondary != NULL );

	t( sp_open(env) == 0 );

	void *tx;
	void *po, *so;
	uint32_t a, b;

	tx = sp_begin(env);
	a = 0;
	b = 3;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	a = 1;
	b = 2;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	a = 2;
	b = 1;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	a = 3;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	uint32_t current_a = 0;
	uint32_t current_b = 3;
	void *cur = sp_cursor(env);
	po = sp_document(primary);
	sp_setstring(po, "order", ">=", 0);
	while ((po = sp_get(cur, po))) {
		t( *(uint32_t*)sp_getstring(po, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(po, "b", NULL) == current_b );
		current_a++;
		current_b--;
	}
	sp_destroy(cur);

	current_a = 3;
	current_b = 0;
	cur = sp_cursor(env);
	so = sp_document(secondary);
	sp_setstring(so, "order", ">=", 0);
	while ((so = sp_get(cur, so))) {
		t( *(uint32_t*)sp_getstring(so, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(so, "b", NULL) == current_b );
		current_a--;
		current_b++;
	}
	sp_destroy(cur);

	sp_destroy(env);
}

static void
secondary_index_test_unique1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	/* unique */
	t( sp_setstring(env, "db", "primary", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.a", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.b", "u32", 0) == 0 );
	t( sp_setint(env, "db.primary.sync", 0) == 0 );

	/* unique */
	t( sp_setstring(env, "db", "secondary", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.a", "u32", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.b", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.secondary.sync", 0) == 0 );

	void *primary = sp_getobject(env, "db.primary");
	void *secondary = sp_getobject(env, "db.secondary");

	t( primary != NULL );
	t( secondary != NULL );

	t( sp_open(env) == 0 );

	void *tx;
	void *po, *so;
	uint32_t a, b;

	tx = sp_begin(env);
	a = 0;
	b = 3;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	tx = sp_begin(env);
	a = 1;
	b = 2;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	tx = sp_begin(env);
	a = 2;
	b = 1;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	tx = sp_begin(env);
	a = 3;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	uint32_t current_a = 0;
	uint32_t current_b = 3;
	void *cur = sp_cursor(env);
	po = sp_document(primary);
	sp_setstring(po, "order", ">=", 0);
	while ((po = sp_get(cur, po))) {
		t( *(uint32_t*)sp_getstring(po, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(po, "b", NULL) == current_b );
		current_a++;
		current_b--;
	}
	sp_destroy(cur);

	current_a = 3;
	current_b = 0;
	cur = sp_cursor(env);
	so = sp_document(secondary);
	sp_setstring(so, "order", ">=", 0);
	while ((so = sp_get(cur, so))) {
		t( *(uint32_t*)sp_getstring(so, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(so, "b", NULL) == current_b );
		current_a--;
		current_b++;
	}
	sp_destroy(cur);

	sp_destroy(env);
}

static void
secondary_index_test_nonunique0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	/* unique */
	t( sp_setstring(env, "db", "primary", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.a", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.b", "u32", 0) == 0 );
	t( sp_setint(env, "db.primary.sync", 0) == 0 );

	/* non-unique */
	t( sp_setstring(env, "db", "secondary", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.a", "u32,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.b", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.secondary.sync", 0) == 0 );

	void *primary = sp_getobject(env, "db.primary");
	void *secondary = sp_getobject(env, "db.secondary");

	t( primary != NULL );
	t( secondary != NULL );

	t( sp_open(env) == 0 );

	void *tx;
	void *po, *so;
	uint32_t a, b;

	tx = sp_begin(env);
	a = 0;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	a = 1;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	a = 2;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	tx = sp_begin(env);
	a = 3;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );

	t( sp_setint(env, "db.primary.branch", 0) == 0 );
	t( sp_setint(env, "db.secondary.branch", 0) == 0 );

	uint32_t current_a = 0;
	uint32_t current_b = 0;
	void *cur = sp_cursor(env);
	po = sp_document(primary);
	sp_setstring(po, "order", ">=", 0);
	while ((po = sp_get(cur, po))) {
		t( *(uint32_t*)sp_getstring(po, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(po, "b", NULL) == current_b );
		current_a++;
	}
	sp_destroy(cur);

	current_a = 0;
	current_b = 0;
	cur = sp_cursor(env);
	so = sp_document(secondary);
	sp_setstring(so, "order", ">=", 0);
	while ((so = sp_get(cur, so))) {
		t( *(uint32_t*)sp_getstring(so, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(so, "b", NULL) == current_b );
		current_a++;
	}
	sp_destroy(cur);

	sp_destroy(env);
}

static void
secondary_index_test_nonunique1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "compaction.0.branch_wm", 1) == 0 );

	/* unique */
	t( sp_setstring(env, "db", "primary", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.a", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.primary.scheme.b", "u32", 0) == 0 );
	t( sp_setint(env, "db.primary.sync", 0) == 0 );

	/* non-unique */
	t( sp_setstring(env, "db", "secondary", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "a", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.a", "u32,key(1)", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme", "b", 0) == 0 );
	t( sp_setstring(env, "db.secondary.scheme.b", "u32,key(0)", 0) == 0 );
	t( sp_setint(env, "db.secondary.sync", 0) == 0 );

	void *primary = sp_getobject(env, "db.primary");
	void *secondary = sp_getobject(env, "db.secondary");

	t( primary != NULL );
	t( secondary != NULL );

	t( sp_open(env) == 0 );

	void *tx;
	void *po, *so;
	uint32_t a, b;

	tx = sp_begin(env);
	a = 0;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	tx = sp_begin(env);
	a = 1;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	tx = sp_begin(env);
	a = 2;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	tx = sp_begin(env);
	a = 3;
	b = 0;
	po = sp_document(primary);
	sp_setstring(po, "a", &a, sizeof(a));
	sp_setstring(po, "b", &b, sizeof(b));
	t( sp_open(po) == 0 );
	t( sp_set(tx, po) == 0 );
	so = sp_document(secondary);
	t( sp_setobject(so, "reuse", po) == 0 );
	t( sp_set(tx, so) == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(po);

	t( sp_setint(env, "db.primary.branch", 0) == 0 );
	t( sp_setint(env, "db.secondary.branch", 0) == 0 );

	uint32_t current_a = 0;
	uint32_t current_b = 0;
	void *cur = sp_cursor(env);
	po = sp_document(primary);
	sp_setstring(po, "order", ">=", 0);
	while ((po = sp_get(cur, po))) {
		t( *(uint32_t*)sp_getstring(po, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(po, "b", NULL) == current_b );
		current_a++;
	}
	sp_destroy(cur);

	current_a = 0;
	current_b = 0;
	cur = sp_cursor(env);
	so = sp_document(secondary);
	sp_setstring(so, "order", ">=", 0);
	while ((so = sp_get(cur, so))) {
		t( *(uint32_t*)sp_getstring(so, "a", NULL) == current_a );
		t( *(uint32_t*)sp_getstring(so, "b", NULL) == current_b );
		current_a++;
	}
	sp_destroy(cur);

	sp_destroy(env);
}

stgroup *secondary_index_group(void)
{
	stgroup *group = st_group("secondary_index");
	st_groupadd(group, st_test("test0", secondary_index_test0));
	st_groupadd(group, st_test("unique", secondary_index_test_unique0));
	st_groupadd(group, st_test("unique_reuse", secondary_index_test_unique1));
	st_groupadd(group, st_test("nonunique", secondary_index_test_nonunique0));
	st_groupadd(group, st_test("nonunique_reuse", secondary_index_test_nonunique1));
	return group;
}
