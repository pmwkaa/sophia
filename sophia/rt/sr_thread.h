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

typedef void *(*srthreadf)(void*);

struct srthread {
	pthread_t id;
	void *arg;
};

int sr_threadnew(srthread*, srthreadf, void*);
int sr_threadjoin(srthread*);

#endif
