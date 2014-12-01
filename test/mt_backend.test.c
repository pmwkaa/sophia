
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

#if 0
	printf(" (checkpoint)");
	void *c = sp_ctl(cx->env);
	t( sp_set(c, "scheduler.run") == 0 );
#endif

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

stgroup *mt_backend_group(void)
{
	stgroup *group = st_group("mt_backend");
	st_groupadd(group, st_test("setget", mt_setget));
	return group;
}
