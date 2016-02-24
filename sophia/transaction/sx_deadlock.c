
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libsx.h>

static inline int
sx_deadlock_in(sxmanager *m, sslist *mark, sx *t, sx *p)
{
	if (p->deadlock.next != &p->deadlock)
		return 0;
	ss_listappend(mark, &p->deadlock);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &p->log->buf, sizeof(svlogv));
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		if (v->prev == NULL)
			continue;
		do {
			sx *n = sx_find(m, v->id);
			assert(n != NULL);
			if (ssunlikely(n == t))
				return 1;
			int rc = sx_deadlock_in(m, mark, t, n);
			if (ssunlikely(rc == 1))
				return 1;
			v = v->prev;
		} while (v);
	}
	return 0;
}

static inline void
sx_deadlock_unmark(sslist *mark)
{
	sslist *i, *n;
	ss_listforeach_safe(mark, i, n) {
		sx *t = sscast(i, sx, deadlock);
		ss_listinit(&t->deadlock);
	}
}

int sx_deadlock(sx *t)
{
	sxmanager *m = t->manager;
	sslist mark;
	ss_listinit(&mark);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &t->log->buf, sizeof(svlogv));
	while (ss_iterhas(ss_bufiter, &i))
	{
		svlogv *lv = ss_iterof(ss_bufiter, &i);
		sxv *v = lv->v.v;
		if (v->prev == NULL) {
			ss_iternext(ss_bufiter, &i);
			continue;
		}
		sx *p = sx_find(m, v->prev->id);
		assert(p != NULL);
		int rc = sx_deadlock_in(m, &mark, t, p);
		if (ssunlikely(rc)) {
			sx_deadlock_unmark(&mark);
			return 1;
		}
		ss_iternext(ss_bufiter, &i);
	}
	sx_deadlock_unmark(&mark);
	return 0;
}
