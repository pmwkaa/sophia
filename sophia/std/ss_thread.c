
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

static inline void
ss_threadinit(ssthread *t)
{
	ss_listinit(&t->link);
	t->f = NULL;
	memset(&t->id, 0, sizeof(t->id));
}

static inline int
ss_threadnew(ssthread *t, ssthreadf f, void *arg)
{
	t->arg = arg;
	t->f = f;
	return pthread_create(&t->id, NULL, f, t);
}

static inline int
ss_threadjoin(ssthread *t)
{
	return pthread_join(t->id, NULL);
}

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
		rc = ss_threadjoin(t);
		if (ssunlikely(rc == -1))
			rcret = -1;
		ss_free(a, t);
	}
	return rcret;
}

int ss_threadpool_new(ssthreadpool *p, ssa *a, int n, ssthreadf f, void *arg)
{
	int id = 0;
	int i;
	for (i = 0; i < n; i++) {
		ssthread *t = ss_malloc(a, sizeof(*t));
		if (ssunlikely(t == NULL))
			goto error;
		t->id = id++;
		ss_listappend(&p->list, &t->link);
		p->n++;
		int rc = ss_threadnew(t, f, arg);
		if (ssunlikely(rc == -1))
			goto error;
	}
	return 0;
error:
	ss_threadpool_shutdown(p, a);
	return -1;
}
