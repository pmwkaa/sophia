#ifndef SC_WORKER_H_
#define SC_WORKER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct scworkerpool scworkerpool;
typedef struct scworker scworker;

struct scworker {
	char name[16];
	sstrace trace;
	sdc dc;
	sslist link;
	sslist linkidle;
} sspacked;

struct scworkerpool {
	ssspinlock lock;
	sslist list;
	sslist listidle;
	int total;
	int idle;
};

int sc_workerpool_init(scworkerpool*);
int sc_workerpool_free(scworkerpool*, sr*);
int sc_workerpool_new(scworkerpool*, sr*);

static inline scworker*
sc_workerpool_pop(scworkerpool *p, sr *r)
{
	ss_spinlock(&p->lock);
	if (sslikely(p->idle >= 1))
		goto pop_idle;
	int rc = sc_workerpool_new(p, r);
	if (ssunlikely(rc == -1)) {
		ss_spinunlock(&p->lock);
		return NULL;
	}
	assert(p->idle >= 1);
pop_idle:;
	scworker *w =
		sscast(ss_listpop(&p->listidle),
		       scworker, linkidle);
	p->idle--;
	ss_spinunlock(&p->lock);
	return w;
}

static inline void
sc_workerpool_push(scworkerpool *p, scworker *w)
{
	ss_spinlock(&p->lock);
	ss_listpush(&p->listidle, &w->linkidle);
	p->idle++;
	ss_spinunlock(&p->lock);
}

#endif
