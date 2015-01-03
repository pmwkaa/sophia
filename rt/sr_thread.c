
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
	t->arg = arg;
	return pthread_create(&t->id, NULL, f, t);
}

int sr_threadjoin(srthread *t)
{
	return pthread_join(t->id, NULL);
}
