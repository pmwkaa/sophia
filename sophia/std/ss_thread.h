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

typedef void *(*ssthreadf)(void*);

struct ssthread {
	pthread_t id;
	void *arg;
};

int ss_threadnew(ssthread*, ssthreadf, void*);
int ss_threadjoin(ssthread*);

#endif
