#ifndef SS_THREAD_H_
#define SS_THREAD_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssthread ssthread;
typedef struct ssthreadpool ssthreadpool;

typedef void *(*ssthreadf)(void*);

struct ssthread {
	pthread_t id;
	ssthreadf f;
	void *arg;
	sslist link;
};

struct ssthreadpool {
	sslist list;
	int n;
};

int ss_threadpool_init(ssthreadpool*);
int ss_threadpool_shutdown(ssthreadpool*, ssa*);
int ss_threadpool_new(ssthreadpool*, ssa*, int, ssthreadf, void*);

#endif
