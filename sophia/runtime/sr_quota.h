#ifndef SR_QUOTA_H_
#define SR_QUOTA_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srquota srquota;

struct srquota {
	uint64_t limit;
	int enable;
};

static inline void
sr_quotainit(srquota *q)
{
	q->limit = 0;
	q->enable = 0;
}

static inline void
sr_quotaenable(srquota *q, int on)
{
	q->enable = on;
}

static inline void
sr_quotaset(srquota *q, uint64_t limit)
{
	q->limit = limit;
}

static inline int
sr_quotaused_percent(srquota *q, srstat *s)
{
	if (q->limit == 0)
		return 0;
	ss_spinlock(&s->lock);
	int percent = (s->v_allocated * 100) / q->limit;
	ss_spinunlock(&s->lock);
	return percent;
}

static inline int
sr_quota(srquota *q, srstat *s)
{
	if (!q->enable || !q->limit)
		return 0;
	ss_spinlock(&s->lock);
	int hit = s->v_allocated >= q->limit;
	ss_spinunlock(&s->lock);
	return hit;
}

#endif
