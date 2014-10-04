
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

#if 0
static inline void
print_current(int i) {
	if (i > 0 && (i % 100000) == 0) {
		printf("%.1fM\n", i / 1000000.0);
	}
}

static void
setget(stc *cx)
{
	char value[100];
	memset(value, 0, sizeof(value));
	uint32_t n = 1000000;
	uint32_t i, k;
	srand(82351);
	srhistogram h;
	sr_histogram_init(&h, 3);
	sr_histogram_clean(&h);
	for (i = 0; i < n; i++) {
		k = rand();
		double t0 = sr_histogram_time();
		*(uint32_t*)value = k;
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		t( sp_set(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(cx->db, o) == 0 );
		double t1 = sr_histogram_time();
		double td = t1 - t0;
		sr_histogram_add(&h, td);
		print_current(i);
	}
	sr_histogram_print(&h);
	srand(82351);
	sr_histogram_clean(&h);
	for (i = 0; i < n; i++) {
		k = rand();
		double t0 = sr_histogram_time();
		void *o = sp_object(cx->db);
		t( o != NULL );
		t( sp_set(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(cx->db, o);
		t( o != NULL );
		t( *(uint32_t*)sp_get(o, "value", NULL) == k );
		double t1 = sr_histogram_time();
		double td = t1 - t0;
		sr_histogram_add(&h, td);
		sp_destroy(o);
		print_current(i);
	}
	sr_histogram_print(&h);
}
#endif

static inline void
print_current(int i) {
	if (i > 0 && (i % 100000) == 0) {
		printf(" %.1fM", i / 1000000.0);
		fflush(NULL);
	}
}

static void
setget(stc *cx)
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
		print_current(i);
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
		print_current(i);
	}
}

stgroup *multithread_group(void)
{
	stgroup *group = st_group("multithread");
	st_groupadd(group, st_test("setget", setget));
	return group;
}
