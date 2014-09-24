#ifndef SR_HISTORGRAM_H_
#define SR_HISTORGRAM_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srhistogram srhistogram;

struct srhistogram {
	int power;
	double min;
	double max;
	double sum;
	double sumsq;
	size_t size;
	size_t buckets[155];
};

double
sr_histogram_time(void);

void sr_histogram_init(srhistogram*, int);
void sr_histogram_clean(srhistogram*);
void sr_histogram_add(srhistogram*, double);
void sr_histogram_print(srhistogram*);

#endif
