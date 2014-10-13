
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>

static inline int
sm_deadlock_in(sm *c, srlist *mark, smtx *t, smtx *p)
{
	if (p->deadlock.next != &p->deadlock)
		return 0;
	sr_listappend(mark, &p->deadlock);
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c->r);
	sr_iteropen(&i, &p->log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		svv *v = vp->v;
		if (v->prev == NULL)
			continue;
		do {
			smtx *n = sm_find(c, v->id.tx.id);
			assert(n != NULL);
			if (srunlikely(n == t))
				return 1;
			int rc = sm_deadlock_in(c, mark, t, n);
			if (srunlikely(rc == 1))
				return 1;
			v = v->prev;
		} while (v);
	}
	return 0;
}

static inline void
sm_deadlock_unmark(srlist *mark)
{
	srlist *i, *n;
	sr_listforeach_safe(mark, i, n) {
		smtx *t = srcast(i, smtx, deadlock);
		sr_listinit(&t->deadlock);
	}
}

int sm_deadlock(smtx *t)
{
	sm *c = t->c;
	srlist mark;
	sr_listinit(&mark);

	sriter i;
	sr_iterinit(&i, &sr_bufiter, c->r);
	sr_iteropen(&i, &t->log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		svv *v = vp->v;
		if (v->prev == NULL)
			continue;
		smtx *p = sm_find(c, v->prev->id.tx.id);
		assert(p != NULL);
		int rc = sm_deadlock_in(c, &mark, t, p);
		if (srunlikely(rc)) {
			sm_deadlock_unmark(&mark);
			return 1;
		}
	}
	sm_deadlock_unmark(&mark);
	return 0;
}
