#ifndef SO_PROFILER_H_
#define SO_PROFILER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sodbprofiler sodbprofiler;

struct sodbprofiler {
	soobj o;
	void *db;
	siprofiler prof;
};

void so_dbprofiler_init(sodbprofiler*, void*);

#endif
