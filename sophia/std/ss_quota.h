#ifndef SS_QUOTA_H_
#define SS_QUOTA_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssquota ssquota;

typedef enum ssquotaop {
	SS_QADD,
	SS_QREMOVE
} ssquotaop;

struct ssquota {
	int enable;
	int wait;
	uint64_t limit;
	uint64_t used;
	ssmutex lock;
	sscond cond;
};

int ss_quotainit(ssquota*);
int ss_quotaset(ssquota*, uint64_t);
int ss_quotaenable(ssquota*, int);
int ss_quotafree(ssquota*);
int ss_quota(ssquota*, ssquotaop, uint64_t);

static inline uint64_t
ss_quotaused(ssquota *q)
{
	ss_mutexlock(&q->lock);
	uint64_t used = q->used;
	ss_mutexunlock(&q->lock);
	return used;
}

static inline int
ss_quotaused_percent(ssquota *q)
{
	ss_mutexlock(&q->lock);
	int percent;
	if (q->limit == 0) {
		percent = 0;
	} else {
		percent = (q->used * 100) / q->limit;
	}
	ss_mutexunlock(&q->lock);
	return percent;
}

#endif
