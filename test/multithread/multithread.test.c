
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
#include <libso.h>
#include <libsv.h>
#include <libsw.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libsc.h>
#include <libse.h>
#include <libst.h>

static inline void *single_stmt_thread(void *arg) 
{
	ssthread *self = arg;
	void *db = self->arg;
	int i = 0;
	while (i < 20000) {
		char key[100];
		int keylen = snprintf(key, sizeof(key), "key_%" PRIiPTR "_%d",
		                      (uintptr_t)self, i);
		void *o = sp_document(db);
		assert(o != NULL);
		sp_setstring(o, "key", key, keylen + 1);
		sp_setstring(o, "value", &i, sizeof(i));
		int rc = sp_set(db, o);
		assert(rc != -1);
		i++;
	}
	return NULL;
}

static void
mt_single_stmt(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 3) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	ssthreadpool p;
	ss_threadpool_init(&p);
	t( ss_threadpool_new(&p, &st_r.a, 5, single_stmt_thread, db) == 0 );
	t( ss_threadpool_shutdown(&p, &st_r.a) == 0 );

	t (sp_getint(env, "db.test.index.count") == 100000 );

	t( sp_destroy(env) == 0 );
}

static inline void *multi_stmt_thread(void *arg) 
{
	ssthread *self = arg;
	void *env = ((void**)self->arg)[0];
	void *db  = ((void**)self->arg)[1];
	int i = 0;
	while (i < 2000) {
		int rc;
		void *tx = sp_begin(env);
		assert( tx != NULL );
		int j = 0;
		while (j < 10) {
			char key[100];
			int keylen = snprintf(key, sizeof(key), "key_%" PRIiPTR "_%d_%d",
			                      (uintptr_t)self, i, j);
			void *o = sp_document(db);
			assert(o != NULL);
			sp_setstring(o, "key", key, keylen + 1);
			rc = sp_set(tx, o);
			assert(rc != -1);
			j++;
		}
		rc = sp_commit(tx);
		assert(rc == 0);
		i++;
	}
	return NULL;
}

static void
mt_multi_stmt(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 3) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	ssthreadpool p;
	ss_threadpool_init(&p);
	void *ptr[2] = { env, db };
	t( ss_threadpool_new(&p, &st_r.a, 5, multi_stmt_thread, ptr) == 0 );
	t( ss_threadpool_shutdown(&p, &st_r.a) == 0 );

	t (sp_getint(env, "db.test.index.count") == 100000 );

	t( sp_destroy(env) == 0 );
}

static inline void *multi_stmt_conflict_thread0(void *arg)
{
	ssthread *self = arg;
	void *env = ((void**)self->arg)[0];
	void *db  = ((void**)self->arg)[1];
	int i = 0;
	while (i < 2000) {
		int rc;
		void *tx = sp_begin(env);
		assert( tx != NULL );
		int j = 0;
		while (j < 10) {
			void *o = sp_document(db);
			int key = i + j;
			assert(o != NULL);
			sp_setstring(o, "key", &key, sizeof(key));
			rc = sp_set(tx, o);
			assert(rc != -1);
			j++;
		}
		rc = sp_commit(tx);
		assert(rc != -1);
		if (rc == 2)
			sp_destroy(tx);
		i++;
	}
	return NULL;
}

static void
mt_multi_stmt_conflict0(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 3) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	/* conflict source */
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	ssthreadpool p;
	ss_threadpool_init(&p);
	void *ptr[2] = { env, db };
	t( ss_threadpool_new(&p, &st_r.a, 5, multi_stmt_conflict_thread0, ptr) == 0 );
	t( ss_threadpool_shutdown(&p, &st_r.a) == 0 );

	t( sp_destroy(env) == 0 );
}

static inline void *multi_stmt_conflict_thread1(void *arg)
{
	ssthread *self = arg;
	void *env = ((void**)self->arg)[0];
	void *db  = ((void**)self->arg)[1];
	int id = 1;
	int rc;
	int i = 0;
	while (i < 100) {
		rc = 0;
		do {
			void *tx = sp_begin(env);
			void *o = sp_document(db);
			sp_setstring(o, "key", &id, sizeof(id));
			o = sp_get(tx, o);
			int v = 0;
			if (o) {
				v = *(int*)sp_getstring(o, "value", NULL);
				sp_destroy(o);
			}
			v++;

			o = sp_document(db);
			sp_setstring(o, "key", &id, sizeof(id));
			sp_setstring(o, "value", &v, sizeof(v));
			rc = sp_set(tx, o);
			assert(rc == 0);

			rc = sp_commit(tx);
			if (2 == rc) sp_destroy(tx);
			assert(rc != -1);
		} while (rc != 0);
		i++;
	}
	return NULL;
}

static void
mt_multi_stmt_conflict1(void)
{
	void *env = sp_env();
	t( env != NULL );
	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 3) == 0 );
	t( sp_setstring(env, "log.path", st_r.conf->log_dir, 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	t( sp_setint(env, "log.sync", 0) == 0 );
	t( sp_setstring(env, "db", "test", 0) == 0 );
	t( sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0) == 0 );
	/* conflict source */
	t( sp_setstring(env, "db.test.scheme", "key", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0) == 0 );
	t( sp_setstring(env, "db.test.scheme", "value", 0) == 0 );
	t( sp_setint(env, "db.test.sync", 0) == 0 );
	void *db = sp_getobject(env, "db.test");
	t( db != NULL );
	t( sp_open(env) == 0 );

	ssthreadpool p;
	ss_threadpool_init(&p);
	void *ptr[2] = { env, db };
	t( ss_threadpool_new(&p, &st_r.a, 3, multi_stmt_conflict_thread1, ptr) == 0 );
	t( ss_threadpool_shutdown(&p, &st_r.a) == 0 );

	int id = 1;
	void *o = sp_document(db);
	sp_setstring(o, "key", &id, sizeof(id));
	o = sp_get(db, o);
	t( o != NULL );
	int v = *(int*)sp_getstring(o, "value", NULL);
	t( v == 300 );
	sp_destroy(o);

	t( sp_destroy(env) == 0 );
}

stgroup *multithread_group(void)
{
	stgroup *group = st_group("mt");
	st_groupadd(group, st_test("single_stmt", mt_single_stmt));
	st_groupadd(group, st_test("multi_stmt", mt_multi_stmt));
	st_groupadd(group, st_test("multi_stmt_conflict0", mt_multi_stmt_conflict0));
	st_groupadd(group, st_test("multi_stmt_conflict1", mt_multi_stmt_conflict1));
	return group;
}
