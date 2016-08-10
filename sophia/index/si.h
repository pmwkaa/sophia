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
	ssmutex    lock;
	siplanner  p;
	ssrb       i;
	int        n;
	uint64_t   update_time;
	uint32_t   backup;
	uint64_t   read_disk;
	uint64_t   read_cache;
	uint64_t   size;
	uint32_t   gc_count;
	sslist     gc;
	sdc        rdc;
	sischeme   scheme;
	so        *object;
	sr         r;
	sslist     link;
};

static inline void
si_lock(si *i) {
	ss_mutexlock(&i->lock);
}

static inline void
si_unlock(si *i) {
	ss_mutexunlock(&i->lock);
}

static inline sr*
si_r(si *i) {
	return &i->r;
}

static inline sischeme*
si_scheme(si *i) {
	return &i->scheme;
}

si *si_init(sr*, so*);
int si_open(si*);
int si_close(si*);
int si_insert(si*, sinode*);
int si_remove(si*, sinode*);
int si_replace(si*, sinode*, sinode*);
int si_execute(si*, sdc*, siplan*, uint64_t);
siplannerrc
si_plan(si*, siplan*);

#endif
