#ifndef SE_WORKER_H_
#define SE_WORKER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seworkerpool seworkerpool;
typedef struct seworker seworker;

struct seworker {
	ssthread t;
	char name[16];
	sstrace trace;
	void *arg;
	sdc dc;
	sslist link;
};

struct seworkerpool {
	sslist list;
	int n;
};

int se_workerpool_init(seworkerpool*);
int se_workerpool_shutdown(seworkerpool*, sr*);
int se_workerpool_new(seworkerpool*, sr*, int, ssthreadf, void*);

static inline void
se_workerstub_init(seworker *w)
{
	sd_cinit(&w->dc);
	ss_listinit(&w->link);
	ss_traceinit(&w->trace);
	ss_trace(&w->trace, "%s", "init");
}

static inline void
se_workerstub_free(seworker *w, sr *r)
{
	sd_cfree(&w->dc, r);
	ss_tracefree(&w->trace);
}

#endif
