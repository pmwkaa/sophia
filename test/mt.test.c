
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libse.h>
#include <libso.h>
#include <libst.h>
#include <sophia.h>

static inline void *single_stmt_thread(void *arg) 
{
	soworker *self = arg;
	void *db = self->arg;
	int i = 0;
	while (i < 20000) {
		char key[100];
		int keylen = snprintf(key, sizeof(key), "key_%" PRIiPTR "_%d",
		                      (uintptr_t)self, i);
		void *o = sp_object(db);
		assert(o != NULL);
		sp_set(o, "key", key, keylen + 1);
		sp_set(o, "value", &i, sizeof(i));
		int rc = sp_set(db, o);
		assert(rc != -1);
		i++;
	}
	return NULL;
}

static void
mt_single_stmt(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "3") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);
	soworkers w;
	so_workersinit(&w);
	t( so_workersnew(&w, &r, 5, single_stmt_thread, cx->db) == 0 );
	t( so_workersshutdown(&w, &r) == 0 );

	void *o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100000") == 0 );
	sp_destroy(o);

	t( sp_destroy(cx->env) == 0 );
}

static inline void *multi_stmt_thread(void *arg) 
{
	soworker *self = arg;
	void *db = self->arg;
	int i = 0;
	while (i < 2000) {
		int rc;
		void *tx = sp_begin(db);
		assert( tx != NULL );
		int j = 0;
		while (j < 10) {
			char key[100];
			int keylen = snprintf(key, sizeof(key), "key_%" PRIiPTR "_%d_%d",
			                      (uintptr_t)self, i, j);
			void *o = sp_object(db);
			assert(o != NULL);
			sp_set(o, "key", key, keylen + 1);
			rc = sp_set(tx, o);
			assert(rc != -1);
			j++;
		}
		rc = sp_commit(tx);
		assert(rc == 0);
		/*
		assert(rc != -1);
		if (rc == 2)
			sp_rollback(tx);
			*/
		i++;
	}
	return NULL;
}

static void
mt_multi_stmt(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "3") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);
	soworkers w;
	so_workersinit(&w);
	t( so_workersnew(&w, &r, 5, multi_stmt_thread, cx->db) == 0 );
	t( so_workersshutdown(&w, &r) == 0 );

	void *o = sp_get(c, "db.test.index.count");
	t( o != NULL );
	t( strcmp( sp_get(o, "value", NULL), "100000") == 0 );
	sp_destroy(o);

	t( sp_destroy(cx->env) == 0 );
}

static inline void *multi_stmt_conflict_thread(void *arg) 
{
	soworker *self = arg;
	void *db = self->arg;
	int i = 0;
	while (i < 2000) {
		int rc;
		void *tx = sp_begin(db);
		assert( tx != NULL );
		int j = 0;
		while (j < 10) {
			void *o = sp_object(db);
			int key = i + j;
			assert(o != NULL);
			sp_set(o, "key", &key, sizeof(key));
			rc = sp_set(tx, o);
			assert(rc != -1);
			j++;
		}
		rc = sp_commit(tx);
		assert(rc != -1);
		if (rc == 2)
			sp_rollback(tx);
		i++;
	}
	return NULL;
}

static void
mt_multi_stmt_conflict(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "3") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	sra a;
	sr_allocopen(&a, &sr_astd);
	srcomparator cmp = { sr_cmpu32, NULL };
	srerror error;
	sr_errorinit(&error);
	sr r;
	sr_init(&r, &error, &a, NULL, &cmp, NULL);
	soworkers w;
	so_workersinit(&w);
	t( so_workersnew(&w, &r, 5, multi_stmt_conflict_thread, cx->db) == 0 );
	t( so_workersshutdown(&w, &r) == 0 );

	t( sp_destroy(cx->env) == 0 );
}

static void
mt_quota(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "memory.limit", "524288") == 0 ); /* 512k */
	t( sp_set(c, "compaction.node_branch_wm", "500000") == 0 ); /* 500k */
	t( sp_set(c, "scheduler.threads", "5") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.cmp", sr_cmpu32) == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );
	char value[1000];
	memset(value, 0, sizeof(value));
	int i = 0;
	while (i < 20000) { /* ~ 20Mb */
		void *o = sp_object(cx->db);
		assert(o != NULL);
		sp_set(o, "key", &i, sizeof(i));
		sp_set(o, "value", value, sizeof(value));
		int rc = sp_set(cx->db, o);
		t( rc == 0 );
		i++;
	}
	t( sp_destroy(cx->env) == 0 );
}

stgroup *mt_group(void)
{
	stgroup *group = st_group("mt");
	st_groupadd(group, st_test("single_stmt", mt_single_stmt));
	st_groupadd(group, st_test("multi_stmt_conflict_less", mt_multi_stmt));
	st_groupadd(group, st_test("multi_stmt_conflict", mt_multi_stmt_conflict));
	st_groupadd(group, st_test("quota", mt_quota));
	return group;
}
