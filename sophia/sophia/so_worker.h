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
	srthread t;
	char name[16];
	srtrace trace;
	void *arg;
	sdc dc;
	srlist link;
};

struct soworkers {
	srlist list;
	int n;
};

int so_workersinit(soworkers*);
int so_workersshutdown(soworkers*, sr*);
int so_workersnew(soworkers*, sr*, int, srthreadf, void*);

static inline void
so_workerstub_init(soworker *w, sr *r)
{
	sd_cinit(&w->dc, r);
	sr_listinit(&w->link);
	sr_traceinit(&w->trace);
	sr_trace(&w->trace, "%s", "init");
}

static inline void
so_workerstub_free(soworker *w, sr *r)
{
	sd_cfree(&w->dc, r);
	sr_tracefree(&w->trace);
}

#endif
