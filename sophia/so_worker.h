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
so_workerwait(soworker *w) {
	sr_threadwait(&w->t);
}

static inline void
so_workerwait_timeout(soworker *w, int secs) {
	sr_threadwait_tm(&w->t, secs);
}

static inline void
so_workerstub_init(soworker *w, sr *r, void *arg)
{
	memset(&w->t, 0, sizeof(w->t));
	w->arg = arg;
	(void)r;
	sd_cinit(&w->dc, r);
	sr_listinit(&w->link);
}

static inline void
so_workerstub_free(soworker *w, sr *r)
{
	(void)w;
	(void)r;
	sd_cfree(&w->dc, r);
}

#endif
