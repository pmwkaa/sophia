
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

int ss_quotainit(ssquota *q)
{
	q->enable = 0;
	q->wait   = 0;
	q->limit  = 0;
	q->used   = 0;
	ss_mutexinit(&q->lock);
	ss_condinit(&q->cond);
	return 0;
}

int ss_quotaset(ssquota *q, uint64_t limit)
{
	q->limit = limit;
	return 0;
}

int ss_quotaenable(ssquota *q, int v)
{
	q->enable = v;
	return 0;
}

int ss_quotafree(ssquota *q)
{
	ss_mutexfree(&q->lock);
	ss_condfree(&q->cond);
	return 0;
}

int ss_quota(ssquota *q, ssquotaop op, uint64_t v)
{
	ss_mutexlock(&q->lock);
	switch (op) {
	case SS_QADD:
		if (ssunlikely(!q->enable || q->limit == 0)) {
			/* .. */
		} else {
			if (ssunlikely((q->used + v) >= q->limit)) {
				q->wait++;
				ss_condwait(&q->cond, &q->lock);
			}
		}
		q->used += v;
		break;
	case SS_QREMOVE:
		q->used -= v;
		if (ssunlikely(q->wait)) {
			q->wait--;
			ss_condsignal(&q->cond);
		}
		break;
	}
	ss_mutexunlock(&q->lock);
	return 0;
}
