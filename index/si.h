#ifndef SI_H_
#define SI_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct si si;

struct si {
	srmutex lock;
	srcond cond;
	siplan plan;
	srrb i;
	uint64_t qos_limit;
	uint64_t qos_used;
	int qos_wait;
	int qos_on;
	int n;
	siconf *conf;
};

static inline void
si_lock(si *i) {
	sr_mutexlock(&i->lock);
}

static inline void
si_unlock(si *i) {
	sr_mutexunlock(&i->lock);
}

int si_init(si*, siconf*);
int si_open(si*, sr*);
int si_close(si*, sr*);
int si_insert(si*, sr*, sinode*);
int si_replace(si*, sinode*, sinode*);

#endif
