
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

void sr_sleep(uint64_t ns)
{
	struct timespec ts;
	ts.tv_sec  = 0;
	ts.tv_nsec = ns;
	nanosleep(&ts, NULL);
}

double sr_time(void)
{
#if defined(CLOCK_MONOTONIC)
	struct timespec t;
	clock_gettime(CLOCK_MONOTONIC, &t);
	return t.tv_sec * 1e-9 + t.tv_nsec;
#else
	struct timeval t;
	gettimeofday(&t, NULL);
	return t.tv_sec * 1e-9 + tv.tv_usec * 1000;
#endif
}
