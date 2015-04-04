
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>

static inline int
sx_deadlock_in(sxmanager *m, srlist *mark, sx *t, sx *p)
{
	if (p->deadlock.next != &p->deadlock)
		return 0;
	sr_listappend(mark, &p->deadlock);
	sriter i;
	sr_iterinit(sr_bufiter, &i, m->r);
	sr_iteropen(sr_bufiter, &i, &p->log.buf, sizeof(svlogv));
	for (; sr_iterhas(sr_bufiter, &i); sr_iternext(sr_bufiter, &i))
	{
		svlogv *lv = sr_iterof(sr_bufiter, &i);
		sxv *v = lv->v.v;
		if (v->prev == NULL)
			continue;
		do {
			sx *n = sx_find(m, v->id);
			assert(n != NULL);
			if (srunlikely(n == t))
				return 1;
			int rc = sx_deadlock_in(m, mark, t, n);
			if (srunlikely(rc == 1))
				return 1;
			v = v->prev;
		} while (v);
	}
	return 0;
}

static inline void
sx_deadlock_unmark(srlist *mark)
{
	srlist *i, *n;
	sr_listforeach_safe(mark, i, n) {
		sx *t = srcast(i, sx, deadlock);
		sr_listinit(&t->deadlock);
	}
}

int sx_deadlock(sx *t)
{
	sxmanager *m = t->manager;
	srlist mark;
	sr_listinit(&mark);
	sriter i;
	sr_iterinit(sr_bufiter, &i, m->r);
	sr_iteropen(sr_bufiter, &i, &t->log.buf, sizeof(svlogv));
	while (sr_iterhas(sr_bufiter, &i))
	{
		svlogv *lv = sr_iterof(sr_bufiter, &i);
		sxv *v = lv->v.v;
		if (v->prev == NULL) {
			sr_iternext(sr_bufiter, &i);
			continue;
		}
		sx *p = sx_find(m, v->prev->id);
		assert(p != NULL);
		int rc = sx_deadlock_in(m, &mark, t, p);
		if (srunlikely(rc)) {
			sx_deadlock_unmark(&mark);
			return 1;
		}
		sr_iternext(sr_bufiter, &i);
	}
	sx_deadlock_unmark(&mark);
	return 0;
}
