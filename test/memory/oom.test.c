
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

static inline void*
oom_open(void *env)
{
	int rc;
	rc = sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0);
	if (rc == -1)
		return NULL;
	rc = sp_setint(env, "scheduler.threads", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setint(env, "compaction.0.branch_wm", 1);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "log.path", st_r.conf->log_dir, 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db", "test", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.index.key", "u32", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setint(env, "db.test.sync", 0);
	if (rc == -1)
		return NULL;
	void *db = sp_getobject(env, "db.test");
	if (db == NULL)
		return NULL;
	rc = sp_open(env);
	if (rc == -1)
		return NULL;
	return db;
}

static inline int
oom_write_read(void *env, void *db)
{
	/* write */
	void *o = sp_object(db);
	if (o == NULL)
		return -1;
	uint32_t key = 123;
	int rc;
	rc = sp_setstring(o, "key", &key, sizeof(key));
	if (rc == -1) {
		sp_destroy(o);
		return -1;
	}
	rc = sp_setstring(o, "value", &key, sizeof(key));
	if (rc == -1) {
		sp_destroy(o);
		return -1;
	}
	rc = sp_set(db, o);
	if (rc == -1)
		return -1;
	/* read */
	o = sp_object(db);
	if (o == NULL)
		return -1;
	rc = sp_setstring(o, "key", &key, sizeof(key));
	if (rc == -1) {
		sp_destroy(o);
		return -1;
	}
	o = sp_get(db, o);
	if (o == NULL)
		return -1;
	sp_destroy(o);
	/* cursor */
	void *c = sp_cursor(env);
	if (c == NULL)
		return -1;
	o = sp_object(db);
	if (o == NULL) {
		sp_destroy(c);
		return -1;
	}
	o = sp_get(c, o);
	if (o == NULL) {
		sp_destroy(c);
		return -1;
	}
	sp_destroy(o);
	sp_destroy(c);
	return 0;
}

static inline int
oom_compaction(void *env, void *db)
{
	/* branch oom */
	int rc = sp_setint(env, "db.test.branch", 0);
	if (rc == -1)
		return -1;
	uint32_t key = 123;
	int count = 0;
	while (count < 10) {
		void *o = sp_object(db);
		if (o == NULL)
			return -1;
		rc = sp_setstring(o, "key", &key, sizeof(key));
		if (rc == -1) {
			sp_destroy(o);
			return -1;
		}
		rc = sp_setstring(o, "value", &key, sizeof(key));
		if (rc == -1) {
			sp_destroy(o);
			return -1;
		}
		rc = sp_set(db, o);
		if (rc == -1)
			return -1;
		key++;
		count++;
	}
	rc = sp_setint(env, "db.test.branch", 0);
	if (rc == -1)
		return -1;
	/* left some in log */
	while (count < 15) {
		void *o = sp_object(db);
		if (o == NULL)
			return -1;
		rc = sp_setstring(o, "key", &key, sizeof(key));
		if (rc == -1) {
			sp_destroy(o);
			return -1;
		}
		rc = sp_setstring(o, "value", &key, sizeof(key));
		if (rc == -1) {
			sp_destroy(o);
			return -1;
		}
		rc = sp_set(db, o);
		if (rc == -1)
			return -1;
		key++;
		count++;
	}
	/* compaction oom */
	rc = sp_setint(env, "db.test.compact", 0);
	if (rc == -1)
		return -1;
	return 0;
}

static void
oom_test(void)
{
	int i = 0;
	int j = 0;
	for (;; i++) {
		st_scene_rmrf(NULL);
		fprintf(st_r.output, " %d", i);
		fflush(NULL);
		/* open */
		void *env = sp_env();
		t( env != NULL );
		t( sp_setint(env, "debug.error_injection.oom", i) == 0 );
		void *db = oom_open(env);
		if (db == NULL) {
			t( sp_destroy(env) == 0 );
			continue;
		}
		/* write, read, get, cursor */
		int rc = oom_write_read(env, db);
		if (rc == -1) {
			t( sp_destroy(env) == 0 );
			continue;
		}
		/* branch + compaction */
		rc = oom_compaction(env, db);
		if (rc == -1) {
			t( sp_destroy(env) == 0 );
			continue;
		}
		t( sp_destroy(env) == 0 );
		/* recover */
		env = sp_env();
		t( env != NULL );
		t( sp_setint(env, "debug.error_injection.oom", j) == 0 );
		db = oom_open(env);
		if (db == NULL) {
			j++;
			t( sp_destroy(env) == 0 );
			continue;
		}
		t( sp_destroy(env) == 0 );
		break;
	}
}

stgroup *oom_group(void)
{
	stgroup *group = st_group("oom");
	st_groupadd(group, st_test("test", oom_test));
	return group;
}
