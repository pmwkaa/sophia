
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

int sr_threadnew(srthread *t, srthreadf f, void *arg)
{
	int rc = pthread_mutex_init(&t->lock, NULL);
	if (srunlikely(rc == -1))
		goto e0;
	rc = pthread_cond_init(&t->cond, NULL);
	if (srunlikely(rc == -1))
		goto e0;
	t->arg = arg;
	rc = pthread_create(&t->id, NULL, f, t);
	if (srunlikely(rc == -1))
		goto e1;
	return 0;
e1:
	pthread_cond_destroy(&t->cond);
e0:
	pthread_mutex_destroy(&t->lock);
	return -1;
}

int sr_threadjoin(srthread *t)
{
	int rc = pthread_join(t->id, NULL);
	if (rc == -1)
		return -1;
	pthread_cond_destroy(&t->cond);
	pthread_mutex_destroy(&t->lock);
	return rc;
}

void sr_threadwakeup(srthread *t)
{
	pthread_mutex_lock(&t->lock);
	pthread_cond_signal(&t->cond);
	pthread_mutex_unlock(&t->lock);
}

void sr_threadwait(srthread *t)
{
	pthread_mutex_lock(&t->lock);
	pthread_cond_wait(&t->cond, &t->lock);
	pthread_mutex_unlock(&t->lock);
}

void sr_threadwait_tm(srthread *t, int secs)
{
	struct timespec tm;
	struct timeval now;
	gettimeofday(&now, NULL);
	tm.tv_sec  = now.tv_sec + secs;
	tm.tv_nsec = now.tv_usec * 1000;
	pthread_mutex_lock(&t->lock);
	pthread_cond_timedwait(&t->cond, &t->lock, &tm);
	pthread_mutex_unlock(&t->lock);
}
