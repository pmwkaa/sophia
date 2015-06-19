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
	ssmutex lock;
	siplanner p;
	ssrb i;
	int n;
	int destroyed;
	uint32_t backup;
	uint64_t update_time;
	uint64_t read_disk;
	uint64_t read_cache;
	ssbuf readbuf;
	sischeme *scheme;
	sr *r;
};

static inline void
si_lock(si *i) {
	ss_mutexlock(&i->lock);
}

static inline void
si_unlock(si *i) {
	ss_mutexunlock(&i->lock);
}

int si_init(si*, sr*);
int si_open(si*, sischeme*);
int si_close(si*);
int si_insert(si*, sinode*);
int si_remove(si*, sinode*);
int si_replace(si*, sinode*, sinode*);
int si_plan(si*, siplan*);
int si_execute(si*, sdc*, siplan*, uint64_t);

#endif
