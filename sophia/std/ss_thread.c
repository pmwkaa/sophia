
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

int ss_threadpool_init(ssthreadpool *p)
{
	ss_listinit(&p->list);
	p->n = 0;
	return 0;
}

int ss_threadpool_shutdown(ssthreadpool *p, ssa *a)
{
	int rcret = 0;
	int rc;
	sslist *i, *n;
	ss_listforeach_safe(&p->list, i, n) {
		ssthread *t = sscast(i, ssthread, link);
		rc = pthread_join(t->id, NULL);
		if (ssunlikely(rc != 0))
			rcret = -1;
		ss_free(a, t);
	}
	return rcret;
}

int ss_threadpool_new(ssthreadpool *p, ssa *a, int n, ssthreadf f, void *arg)
{
	int i;
	for (i = 0; i < n; i++) {
		ssthread *t = ss_malloc(a, sizeof(*t));
		if (ssunlikely(t == NULL))
			goto error;
		ss_listinit(&t->link);
		ss_listappend(&p->list, &t->link);
		p->n++;
		t->f = f;
		t->arg = arg;
		int rc;
		rc = pthread_create(&t->id, NULL, f, t);
		if (ssunlikely(rc != 0))
			goto error;
	}
	return 0;
error:
	ss_threadpool_shutdown(p, a);
	return -1;
}

int ss_thread_setname(ssthread *t, char *name)
{
	#if defined(__APPLE__)
		(void)t;
		return pthread_setname_np(name);
	#else
		return pthread_setname_np(t->id, name);
	#endif
}
