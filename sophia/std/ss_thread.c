
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

int ss_threadnew(ssthread *t, ssthreadf f, void *arg)
{
	t->arg = arg;
	return pthread_create(&t->id, NULL, f, t);
}

int ss_threadjoin(ssthread *t)
{
	return pthread_join(t->id, NULL);
}
