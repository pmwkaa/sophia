
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

static int workflow_upsert_n = 0;

static int
workflow_upsert_operator(int count,
                         char **src,    uint32_t *src_size,
                         char **upsert, uint32_t *upsert_size,
                         char **result, uint32_t *result_size,
                         void *arg)
{
	(void)arg;
	workflow_upsert_n++;
	if (workflow_upsert_n == 1)
		return -1;
	assert(upsert != NULL);
	/* copy value field from upsert */
	result_size[1] = upsert_size[1];
	result[1] = malloc(upsert_size[1]);
	memcpy(result[1], upsert[1], upsert_size[1]);
	return 0;
}

static inline void*
workflow_open(void *env)
{
	int rc;
	rc = sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0);
	if (rc == -1)
		return NULL;
	rc = sp_setint(env, "scheduler.threads", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "log.path", st_r.conf->log_dir, 0);
	if (rc == -1)
		return NULL;
	rc = sp_setint(env, "log.sync", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db", "test", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setint(env, "db.test.compaction.branch_wm", 1);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.path", st_r.conf->db_dir, 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.compression_cold", "lz4", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.scheme", "key", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.scheme.key", "u32,key(0)", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.scheme", "value", 0);
	if (rc == -1)
		return NULL;
	rc = sp_setstring(env, "db.test.upsert", workflow_upsert_operator, 0);
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
workflow_write_read(void *env, void *db)
{
	/* write */
	void *o = sp_document(db);
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

	/* transaction */
	void *tx = sp_begin(env);
	if (tx == NULL)
		return -1;
	o = sp_document(db);
	if (o == NULL)
		return -1;
	key = 123;
	rc = sp_setstring(o, "key", &key, sizeof(key));
	if (rc == -1) {
		sp_destroy(o);
		sp_destroy(tx);
		return -1;
	}
	rc = sp_setstring(o, "value", &key, sizeof(key));
	if (rc == -1) {
		sp_destroy(o);
		sp_destroy(tx);
		return -1;
	}
	rc = sp_set(tx, o);
	if (rc == -1) {
		sp_destroy(tx);
		return -1;
	}
	o = sp_document(db);
	if (o == NULL)
		return -1;
	rc = sp_setstring(o, "key", &key, sizeof(key));
	if (rc == -1) {
		sp_destroy(o);
		sp_destroy(tx);
		return -1;
	}
	o = sp_get(tx, o);
	if (o == NULL) {
		sp_destroy(tx);
		return -1;
	}
	sp_destroy(o);
	rc = sp_commit(tx);
	if (rc == -1)
		return -1;

	/* read */
	o = sp_document(db);
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
	o = sp_document(db);
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
workflow_upsert(void *env, void *db)
{
	void *o = sp_document(db);
	if (o == NULL)
		return -1;
	int up = 777;
	int i = 0;
	sp_setstring(o, "key", &i, sizeof(i));
	sp_setstring(o, "value", &up, sizeof(up));
	int rc = sp_upsert(db, o);
	if (rc == -1)
		return -1;
	o = sp_document(db);
	if (o == NULL)
		return -1;
	up = 778;
	sp_setstring(o, "key", &i, sizeof(i));
	sp_setstring(o, "value", &up, sizeof(up));
	rc = sp_upsert(db, o);
	if (rc == -1)
		return -1;
	o = sp_document(db);
	if (o == NULL)
		return -1;
	sp_setstring(o, "key", &i, sizeof(i));
	o = sp_get(db, o);
	if (o == NULL)
		return -1;
	t( *(int*)sp_getstring(o, "value", NULL) == 778 );
	sp_destroy(o);
	return 0;
}

static inline int
workflow_compaction(void *env, void *db)
{
	/* branch oom */
	int rc = sp_setint(env, "db.test.compaction.branch", 0);
	if (rc == -1)
		return -1;
	uint32_t key = 123;
	int count = 0;
	while (count < 10) {
		void *o = sp_document(db);
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
	rc = sp_setint(env, "db.test.compaction.branch", 0);
	if (rc == -1)
		return -1;
	/* put some statements in log */
	while (count < 15) {
		void *o = sp_document(db);
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
	rc = sp_setint(env, "db.test.compaction.compact", 0);
	if (rc == -1)
		return -1;
	return 0;
}

void
workflow_test(char *injection)
{
	workflow_upsert_n = 0;

	int i = 0;
	int j = 0;
	for (;; i++) {
		st_scene_rmrf(NULL);
		fprintf(st_r.output, " %d", i);
		fflush(NULL);
		/* open */
		void *env = sp_env();
		t( env != NULL );
		t( sp_setint(env, injection, i) == 0 );
		void *db = workflow_open(env);
		if (db == NULL) {
			sp_destroy(env); /* close(2) might fail */
			continue;
		}
		/* write, transaction, read, get, cursor */
		int rc = workflow_write_read(env, db);
		if (rc == -1) {
			sp_destroy(env);
			continue;
		}
		/* upsert */
		rc = workflow_upsert(env, db);
		if (rc == -1) {
			sp_destroy(env);
			continue;
		}
		/* branch + compaction */
		rc = workflow_compaction(env, db);
		if (rc == -1) {
			sp_destroy(env);
			continue;
		}
		sp_destroy(env);
		/* recover */
		env = sp_env();
		t( env != NULL );
		t( sp_setint(env, injection, j) == 0 );
		db = workflow_open(env);
		if (db == NULL) {
			j++;
			sp_destroy(env);
			continue;
		}
		sp_destroy(env);
		break;
	}
}

/* see io and oom tests */
