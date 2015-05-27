#ifndef SO_WORKER_H_
#define SO_WORKER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soworker soworker;
typedef struct soworkers soworkers;

struct soworker {
	ssthread t;
	char name[16];
	sstrace trace;
	void *arg;
	sdc dc;
	sslist link;
};

struct soworkers {
	sslist list;
	int n;
};

int so_workersinit(soworkers*);
int so_workersshutdown(soworkers*, sr*);
int so_workersnew(soworkers*, sr*, int, ssthreadf, void*);

static inline void
so_workerstub_init(soworker *w)
{
	sd_cinit(&w->dc);
	ss_listinit(&w->link);
	ss_traceinit(&w->trace);
	ss_trace(&w->trace, "%s", "init");
}

static inline void
so_workerstub_free(soworker *w, sr *r)
{
	sd_cfree(&w->dc, r);
	ss_tracefree(&w->trace);
}

#endif
