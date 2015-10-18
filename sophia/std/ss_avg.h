#ifndef SS_AVG_H_
#define SS_AVG_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssavg ssavg;

struct ssavg {
	uint64_t count;
	uint64_t total;
	uint32_t min, max, avg;
	char sz[32];
};

static inline void
ss_avginit(ssavg *a)
{
	a->count = 0;
	a->total = 0;
	a->min = 0;
	a->max = 0;
	a->avg = 0;
}

static inline void
ss_avgupdate(ssavg *a, uint32_t v)
{
	a->count++;
	a->total += v;
	a->avg = a->total / a->count;
	if (v < a->min)
		a->min = v;
	if (v > a->max)
		a->max = v;
}

static inline void
ss_avgprepare(ssavg *a)
{
	snprintf(a->sz, sizeof(a->sz), "%"PRIu32" %"PRIu32" %"PRIu32,
	         a->avg, a->min, a->max);
}

#endif
