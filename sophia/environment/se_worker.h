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
	char name[16];
	sstrace trace;
	sdc dc;
	sslist link;
	sslist linkidle;
};

struct seworkerpool {
	ssspinlock lock;
	sslist list;
	sslist listidle;
	int total;
	int idle;
};

int se_workerpool_init(seworkerpool*);
int se_workerpool_free(seworkerpool*, sr*);
int se_workerpool_new(seworkerpool*, sr*);

static inline seworker*
se_workerpool_pop(seworkerpool *p, sr *r)
{
	ss_spinlock(&p->lock);
	if (sslikely(p->idle >= 1))
		goto pop_idle;
	int rc = se_workerpool_new(p, r);
	if (ssunlikely(rc == -1)) {
		ss_spinunlock(&p->lock);
		return NULL;
	}
	assert(p->idle >= 1);
pop_idle:;
	seworker *w =
		sscast(ss_listpop(&p->listidle),
		       seworker, linkidle);
	p->idle--;
	ss_spinunlock(&p->lock);
	return w;
}

static inline void
se_workerpool_push(seworkerpool *p, seworker *w)
{
	ss_spinlock(&p->lock);
	ss_listpush(&p->listidle, &w->linkidle);
	p->idle++;
	ss_spinunlock(&p->lock);
}

#endif
