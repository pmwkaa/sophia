
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <sophia.h>

#include <time.h>
#include <assert.h>
#include <sys/time.h>
#include "suite.h"

unsigned long long now(void)
{
	unsigned long long tm;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	tm = ((long)tv.tv_sec) * 1000;
	tm += tv.tv_usec / 1000;
	return tm;
}

static inline void
print_current(int i, void *db) {
	if (i > 0 && (i % 100000) == 0) {
		printf("%.1fM\n", i / 1000000.0);
		(void)db;
		/*
		int pages = 0;
		int pages_score = 0;
		int total = 0;
		void so_stat(void *obj, int *pages, int *pages_score, int *total);
		so_stat(db, &pages, &pages_score, &total);
		printf("pages: %d, pages_score: %d, keys: %d\n",
		       pages, pages_score, total);
			   */
	}
}

unsigned long long begin = 0;
int n = 0;

void qos(int limit)
{
	if (n == limit) {
		unsigned long long c = now();
		unsigned long long diff = c - begin;
		float rps = n / (diff / 1000.0);

		if (rps > limit) {
			double t = ((rps - limit) * ( (double)diff / (double)n ));
			/*printf("sleep: %f\n", t * 1000.0);*/
			usleep(t * 1000.0);
		}
		begin = now();
		n = 0;
	}

	n++;
}

int
main(int argc, char *argv[])
{
	int op = 1 | 2;
	if (argc == 2) {
		if (strcmp(argv[1], "set") == 0)
			op = 1;
		else
			op = 2;
	}

	void *env = sp_env();
	t( env != NULL );
	void *db = sp_storage(env);
	t( db != NULL );
	void *c = sp_ctl(db, "conf");
	t( c != NULL );
	t( sp_set(c, "storage.dir", "./test") == 0 );
	t( sp_set(c, "storage.logdir", "./log") == 0 );
	t( sp_set(c, "storage.cmp", sr_cmpu32) == 0 );
	t( sp_set(c, "storage.threads", 2) == 0 );
	t( sp_set(c, "storage.memory_limit", 1024 * 1024 * 100ULL) == 0 );
	t( sp_open(env) == 0 );

	char value[100];
	memset(value, 0, sizeof(value));

	uint32_t n = 1000000;
	uint32_t i, k;
	srhistogram h;
	sr_histogram_init(&h, 3);

	if (op & 1) {
		srand(82351);
		sr_histogram_clean(&h);
		for (i = 0; i < n; i++) {
			k = rand();
			double t0 = sr_histogram_time();
			*(uint32_t*)value = k;
			void *o = sp_object(db);
			t( o != NULL );
			t( sp_set(o, "key", &k, sizeof(k)) == 0 );
			t( sp_set(o, "value", value, sizeof(value)) == 0 );
			t( sp_set(db, o) == 0 );
			double t1 = sr_histogram_time();
			double td = t1 - t0;
			sr_histogram_add(&h, td);
			print_current(i, db);
		}
		sr_histogram_print(&h);
	}
	if (op & 2) {
		srand(82351);
		sr_histogram_clean(&h);
		for (i = 0; i < n; i++) {
			k = rand();
			double t0 = sr_histogram_time();

			void *o = sp_object(db);
			t( o != NULL );
			t( sp_set(o, "key", &k, sizeof(k)) == 0 );
			o = sp_get(db, o);
			t( o != NULL );
			t( *(uint32_t*)sp_get(o, "value", NULL) == k );

			double t1 = sr_histogram_time();
			double td = t1 - t0;
			sr_histogram_add(&h, td);
			sp_destroy(o);
			print_current(i, db);
		}
		sr_histogram_print(&h);
	}

	t( sp_destroy(env) == 0 );
	return 0;
}
