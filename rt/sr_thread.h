#ifndef SR_THREAD_H_
#define SR_THREAD_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srthread srthread;
typedef struct srthreadp srthreadp;

typedef void *(*srthreadf)(void*);

struct srthread {
	pthread_t id;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	void *arg;
};

struct srthreadp {
	sra *a;
	srlist list;
	int n;
};

int  sr_threadnew(srthread*, srthreadf, void*);
int  sr_threadjoin(srthread*);
void sr_threadwakeup(srthread*);
void sr_threadwait(srthread*);
void sr_threadwait_tm(srthread*, int);

#endif
