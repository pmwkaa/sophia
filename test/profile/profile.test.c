
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

static inline void
print_current(int i) {
	if (i > 0 && (i % 100000) == 0) {
		fprintf(st_r.output, " %.1fM", i / 1000000.0);
		fflush(st_r.output);
	}
}

static void
profile_set_get(void)
{
	sthistogram h;
	st_histogram_init(&h);

	uint32_t n = 1000000;

	fprintf(st_r.output, "\n\nSET:");
	fflush(st_r.output);

	char value[100];
	memset(value, 0, sizeof(value));
	uint32_t i, k;
	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		*(uint32_t*)value = k;
		double t0 = st_histogram_time();
		void *o = sp_document(st_r.db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		t( sp_setstring(o, "value", value, sizeof(value)) == 0 );
		t( sp_set(st_r.db, o) == 0 );
		double t1 = st_histogram_time();
		double tb = t1 - t0;
		st_histogram_add(&h, tb);
		print_current(i);
	}

	fprintf(st_r.output, "\n");
	fflush(st_r.output);

	st_histogram_print(&h);
	st_histogram_init(&h);

	fprintf(st_r.output, "\nGET:");
	fflush(st_r.output);

	srand(82351);
	for (i = 0; i < n; i++) {
		k = rand();
		double t0 = st_histogram_time();
		void *o = sp_document(st_r.db);
		t( o != NULL );
		t( sp_setstring(o, "key", &k, sizeof(k)) == 0 );
		o = sp_get(st_r.db, o);
		t( o != NULL );
		double t1 = st_histogram_time();
		double tb = t1 - t0;
		st_histogram_add(&h, tb);
		t( *(uint32_t*)sp_getstring(o, "value", NULL) == k );
		sp_destroy(o);
		print_current(i);
	}

	fprintf(st_r.output, "\n");
	fflush(st_r.output);

	st_histogram_print(&h);
}

stgroup *profile_group(void)
{
	stgroup *group = st_group("profile");
	st_groupadd(group, st_test("set_get", profile_set_get));
	return group;
}
