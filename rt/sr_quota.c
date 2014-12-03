
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

int sr_quotainit(srquota *q)
{
	q->enable = 0;
	q->wait   = 0;
	q->limit  = 0;
	q->used   = 0;
	sr_mutexinit(&q->lock);
	sr_condinit(&q->cond);
	return 0;
}

int sr_quotaset(srquota *q, uint64_t limit)
{
	q->limit = limit;
	return 0;
}

int sr_quotaenable(srquota *q, int v)
{
	q->enable = v;
	return 0;
}

int sr_quotafree(srquota *q)
{
	sr_mutexfree(&q->lock);
	sr_condfree(&q->cond);
	return 0;
}

int sr_quotazone(srquota *q)
{
	sr_mutexlock(&q->lock);
	if (srunlikely(q->limit == 0)) {
		sr_mutexunlock(&q->lock);
		return SR_QZONE_0;
	}
	uint64_t proc = (q->used * 100) / q->limit;
	int zone;
	if (proc <= 50)
		zone = SR_QZONE_A;
	else
	if (proc <= 60)
		zone = SR_QZONE_B;
	else
	if (proc <= 70)
		zone = SR_QZONE_C;
	else
	if (proc <= 80)
		zone = SR_QZONE_D;
	else
		zone = SR_QZONE_E;
	sr_mutexunlock(&q->lock);
	return zone;
}

int sr_quota(srquota *q, srquotaop op, uint64_t v)
{
	sr_mutexlock(&q->lock);
	switch (op) {
	case SR_QADD:
		if (srunlikely(!q->enable || q->limit == 0)) {
			/* .. */
		} else {
			if (srunlikely((q->used + v) >= q->limit)) {
				q->wait++;
				sr_condwait(&q->cond, &q->lock);
			}
		}
		q->used += v;
		break;
	case SR_QREMOVE:
		q->used -= v;
		if (srunlikely(q->used < q->limit && q->wait)) {
			q->wait--;
			sr_condsignal(&q->cond);
		}
		break;
	}
	sr_mutexunlock(&q->lock);
	return 0;
}
