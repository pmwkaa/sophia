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

typedef enum srquotaop {
	SR_QADD,
	SR_QREMOVE
} srquotaop;

enum {
	SR_QZONE_0,
	SR_QZONE_A,
	SR_QZONE_B,
	SR_QZONE_C,
	SR_QZONE_D,
	SR_QZONE_E
};

struct srquota {
	int enable;
	int wait;
	uint64_t limit;
	uint64_t used;
	srmutex lock;
	srcond cond;
};

int sr_quotainit(srquota*);
int sr_quotaset(srquota*, uint64_t);
int sr_quotaenable(srquota*, int);
int sr_quotafree(srquota*);
int sr_quotazone(srquota*);
int sr_quota(srquota*, srquotaop, uint64_t);

static inline uint64_t
sr_quotaused(srquota *q)
{
	sr_mutexlock(&q->lock);
	uint64_t used = q->used;
	sr_mutexunlock(&q->lock);
	return used;
}

#endif
