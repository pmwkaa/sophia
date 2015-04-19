
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
#include <time.h>
#include <assert.h>
#include <sys/time.h>

static inline void
print_current(stc *cx srunused, int i) {
	if (i > 0 && (i % 100000) == 0) {
		printf(" %.1fM", i / 1000000.0);
		fflush(NULL);
	}
}

static void
mt_setget(stc *cx)
{
	char value[100];
	memset(value, 0, sizeof(value));
	uint32_t n = 1000000;
	uint32_t i, k;
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		t( sp_set(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(cx->db, o) == 0 );
		print_current(cx, i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(cx->db, o);
		t( o != NULL );
		t( *(uint32_t*)sp_get(o, "value", NULL) == k );
		sp_destroy(o);
		print_current(cx, i);
	}
}

stgroup *mt_backend_multipass_group(void)
{
	stgroup *group = st_group("mt_backend_multipass");
	st_groupadd(group, st_test("setget", mt_setget));
	return group;
}

static void
mt_set_checkpoint_get(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "1") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	char value[100];
	memset(value, 0, sizeof(value));
	uint32_t n = 300000;
	uint32_t i, k;
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		t( sp_set(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(cx->db, o) == 0 );
		print_current(cx, i);
	}
	t( sp_set(c, "log.rotate") == 0 );
	t( sp_set(c, "scheduler.checkpoint") == 0 );
	printf(" (checkpoint..");
	for (;;) {
		void *o = sp_get(c, "scheduler.checkpoint_active");
		t( o != NULL );
		int active = strcmp(sp_get(o, "value", NULL), "1") == 0;
		sp_destroy(o);
		if (!active)
			break;
	}
	printf("done)");

	/* This works only with thread = 1.
	 *
	 * Real data flush can happed before index got
	 * collected and any other worker trigger
	 * checkpoint complete.
	*/
	t( sp_set(c, "log.gc") == 0 );
	void *o = sp_get(c, "log.files");
	t( o != NULL );
	t( strcmp(sp_get(o, "value", NULL), "1") == 0 );
	sp_destroy(o);

	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(cx->db, o);
		t( o != NULL );
		t( *(uint32_t*)sp_get(o, "value", NULL) == k );
		sp_destroy(o);
		print_current(cx, i);
	}

	t( sp_destroy(cx->env) == 0 );
}

static void
mt_set_delete_get(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "5") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	char value[100];
	memset(value, 0, sizeof(value));
	uint32_t n = 700000;
	uint32_t i, k;
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		t( sp_set(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(cx->db, o) == 0 );
		print_current(cx, i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		t( sp_set(o, "value", value, sizeof(value)) == 0 );
		t( sp_delete(cx->db, o) == 0 );
		print_current(cx, i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(cx->db, o);
		t( o == NULL );
		print_current(cx, i);
	}
	t( sp_destroy(cx->env) == 0 );
}

static void
mt_set_get_document_multipart(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "5") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.format", "document") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.key", "u32") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "u32") == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	struct document {
		uint32_t value;
		char used0[89];
		uint32_t key_a;
		char used1[15];
		uint32_t key_b;
		char used2[10];
	} srpacked;
	struct document doc;
	memset(&doc, 'x', sizeof(doc));

	uint32_t n = 500000;
	uint32_t i;

	srand(82351);
	for (i = 0; i < n; i++) {
		doc.key_a = rand();
		doc.key_b = rand();
		doc.value = doc.key_a ^ doc.key_b;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &doc.key_a, sizeof(doc.key_a)) == 0 );
		t( sp_set(o, "key_b", &doc.key_b, sizeof(doc.key_b)) == 0 );
		t( sp_set(o, "value", &doc, sizeof(doc)) == 0 );
		t( sp_set(cx->db, o) == 0 );
		print_current(cx, i);
	}
	srand(82351);
	for (i = 0; i < n; i++) {
		doc.key_a = rand();
		doc.key_b = rand();
		doc.value = doc.key_a ^ doc.key_b;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &doc.key_a, sizeof(doc.key_a)) == 0 );
		t( sp_set(o, "key_b", &doc.key_b, sizeof(doc.key_b)) == 0 );
		o = sp_get(cx->db, o);
		t( o != NULL );

		int valuesize = 0;
		struct document *ret =
			(struct document*)sp_get(o, "value", &valuesize);
		t( valuesize == sizeof(doc) );
		t( doc.key_a == ret->key_a );
		t( doc.key_b == ret->key_b );
		t( doc.value == (ret->key_a ^ ret->key_b) );
		sp_destroy(o);

		print_current(cx, i);
	}

	t( sp_destroy(cx->env) == 0 );
}

static void
mt_set_get_document_multipart_cursor(stc *cx)
{
	cx->env = sp_env();
	t( cx->env != NULL );
	void *c = sp_ctl(cx->env);
	t( c != NULL );
	t( sp_set(c, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(c, "scheduler.threads", "5") == 0 );
	t( sp_set(c, "log.path", cx->suite->logdir) == 0 );
	t( sp_set(c, "log.sync", "0") == 0 );
	t( sp_set(c, "log.rotate_sync", "0") == 0 );
	t( sp_set(c, "db", "test") == 0 );
	t( sp_set(c, "db.test.path", cx->suite->dir) == 0 );
	t( sp_set(c, "db.test.format", "document") == 0 );
	t( sp_set(c, "db.test.sync", "0") == 0 );
	t( sp_set(c, "db.test.index.key", "u32") == 0 );
	t( sp_set(c, "db.test.index", "key_b") == 0 );
	t( sp_set(c, "db.test.index.key_b", "u32") == 0 );
	cx->db = sp_get(c, "db.test");
	t( cx->db != NULL );
	t( sp_open(cx->env) == 0 );

	struct document {
		uint32_t value;
		char used0[89];
		uint32_t key_a;
		char used1[15];
		uint32_t key_b;
		char used2[10];
	} srpacked;
	struct document doc;
	memset(&doc, 'x', sizeof(doc));

	uint32_t n = 500000;
	uint32_t i;

	for (i = 0; i < n; i++) {
		doc.key_a = i;
		doc.key_b = i;
		doc.value = doc.key_a ^ doc.key_b;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &doc.key_a, sizeof(doc.key_a)) == 0 );
		t( sp_set(o, "key_b", &doc.key_b, sizeof(doc.key_b)) == 0 );
		t( sp_set(o, "value", &doc, sizeof(doc)) == 0 );
		t( sp_set(cx->db, o) == 0 );
		print_current(cx, i);
	}

	i = 0;
	void *o = sp_object(cx->db);
	t( o != NULL );
	void *cursor = sp_cursor(cx->db, o);
	t( cursor != NULL );
	while ((o = sp_get(cursor))) {
		int valuesize = 0;
		struct document *ret =
			(struct document*)sp_get(o, "value", &valuesize);
		t( valuesize == sizeof(doc) );
		t( ret->key_a == i );
		t( ret->key_b == i );
		print_current(cx, i);
		i++;
	}
	sp_destroy(cursor);
	t( i == n );

	t( sp_destroy(cx->env) == 0 );
}

stgroup *mt_backend_group(void)
{
	stgroup *group = st_group("mt_backend");
	st_groupadd(group, st_test("set_delete_get", mt_set_delete_get));
	st_groupadd(group, st_test("set_checkpoint_get", mt_set_checkpoint_get));
	st_groupadd(group, st_test("set_get_document_multipart", mt_set_get_document_multipart));
	st_groupadd(group, st_test("set_get_document_multipart_cursor", mt_set_get_document_multipart_cursor));
	return group;
}
