#ifndef ST_HISTOGRAM_H_
#define ST_HISTOGRAM_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define ST_HISTOGRAM_COUNT 155

typedef struct sthistogram sthistogram;

struct sthistogram {
	double min, max, sum;
	double sumsq;
	int    power;
	size_t buckets[ST_HISTOGRAM_COUNT];
	size_t size;
};

static const double
st_histogram_buckets[ST_HISTOGRAM_COUNT] =
{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18,	20,  25,  30,  35,	40,  45,
	50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350,  400,	450,
	500, 600, 700, 800, 900, 1000, 1200, 1400, 1600,  1800,  2000,	2500,  3000,
	3500, 4000, 4500,  5000,  6000,  7000,	8000,  9000,  10000,  12000,  14000,
	16000, 18000, 20000,  25000,  30000,  35000,  40000,  45000,  50000,  60000,
	70000,	80000,	90000,	100000,  120000,  140000,  160000,	180000,  200000,
	250000, 300000, 350000, 400000,  450000,  500000,  600000,	700000,  800000,
	900000, 1000000,  1200000,	1400000,  1600000,	1800000,  2000000,	2500000,
	3000000, 3500000, 4000000,	4500000,  5000000,	6000000,  7000000,	8000000,
	9000000,  10000000,  12000000,	14000000,  16000000,   18000000,   20000000,
	25000000,  30000000,  35000000,  40000000,	45000000,  50000000,   60000000,
	70000000, 80000000, 90000000, 100000000,  120000000,  140000000,  160000000,
	180000000,	 200000000,   250000000,   300000000,	350000000,	  400000000,
	450000000,	 500000000,   600000000,   700000000,	800000000,	  900000000,
	1000000000,  1200000000,  1400000000,  1600000000,	1800000000,  2000000000,
	2500000000.0,  3000000000.0,   3500000000.0,   4000000000.0,   4500000000.0,
	5000000000.0,  6000000000.0,   7000000000.0,   8000000000.0,   9000000000.0,
    1e200, INFINITY
};

static inline void
st_histogram_init(sthistogram *h)
{
	memset(h, 0, sizeof(*h));
	h->min   = INFINITY;
	h->power = 3;
}

static inline void
st_histogram_add(sthistogram *h, double val)
{
	val *= pow(10, h->power);
	size_t begin = 0;
	size_t end = ST_HISTOGRAM_COUNT - 1;
	size_t mid;
	while (1) {
		mid = begin / 2 + end / 2;
		if (mid == begin) {
			for (mid = end; mid > begin; mid--) {
				if (st_histogram_buckets[mid - 1] < val)
					break;
			}
			break;
		}
		if (st_histogram_buckets[mid - 1] < val)
			begin = mid;
		else
			end = mid;
	}
	if (h->min > val)
		h->min = val;
	if (h->max < val)
		h->max = val;
	h->sum += val;
	h->sumsq += val * val;
	h->buckets[mid]++;
	h->size++;
}

static inline void
st_histogram_print(sthistogram *h)
{
	if (h->size == 0)
		return;
	printf("\n");
	printf("[%7s, %7s]\t%11s\t%7s\n", "t min", "t max",
	       "ops count", "%");
	printf("--------------------------------------------------\n");

	int i = 0;
	for (; i < ST_HISTOGRAM_COUNT; i++) {
		if (h->buckets[i] == 0.0)
			continue;
		double percents = (double) h->buckets[i] / h->size;
		printf("[%7.0lf, %7.0lf]\t%11zu\t%7.2lf ",
		       (i > 0) ? st_histogram_buckets[i - 1] : 0.0,
		       st_histogram_buckets[i],
		       h->buckets[i], percents * 1e2);
		putchar('\n');
	}
	double avg_latency = h->sum / h->size;

	printf("--------------------------------------------------\n");
	printf("total:%5s%7.0lf\t%11zu\t   100%%\n", "", h->sum, h->size);
	printf("min latency       : %7.6lf * 1e%d sec/op\n", h->min, -h->power);
	printf("avg latency       : %7.6lf * 1e%d sec/op\n", avg_latency, -h->power);
	printf("max latency       : %7.6lf * 1e%d sec/op\n", h->max, -h->power);

	printf("avg throughput    : %7.0lf ops/sec\n",
	       (double)h->size / (h->sum * pow(10, -h->power)));
}

static inline double
st_histogram_time(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec + 1e-6 * tv.tv_usec;
}

#endif
