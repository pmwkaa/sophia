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

typedef enum {
	SI_REFFE,
	SI_REFBE
} siref;

struct si {
	srstatus   status;
	ssmutex    lock;
	siplanner  p;
	ssrb       i;
	int        n;
	int        destroyed;
	uint64_t   update_time;
	uint32_t   backup;
	uint32_t   snapshot_run;
	uint64_t   snapshot;
	uint64_t   lru_run_lsn;
	uint64_t   lru_v;
	uint64_t   lru_steps;
	uint64_t   lru_intr_lsn;
	uint64_t   lru_intr_sum;
	uint64_t   read_disk;
	uint64_t   read_cache;
	uint64_t   size;
	ssspinlock ref_lock;
	uint32_t   ref_fe;
	uint32_t   ref_be;
	ssbuf      readbuf;
	svupsert   u;
	sischeme  *scheme;
	si        *cache;
	so        *object;
	so         link;
	sr        *r;
};

static inline int
si_active(si *i) {
	return sr_statusactive(&i->status);
}

static inline void
si_lock(si *i) {
	ss_mutexlock(&i->lock);
}

static inline void
si_unlock(si *i) {
	ss_mutexunlock(&i->lock);
}

int si_init(si*, sr*, so*);
int si_open(si*, sischeme*);
int si_close(si*);
int si_insert(si*, sinode*);
int si_remove(si*, sinode*);
int si_replace(si*, sinode*, sinode*);
int si_refs(si*);
int si_refof(si*, siref);
int si_ref(si*, siref);
int si_unref(si*, siref);
int si_plan(si*, siplan*);
int si_execute(si*, sdc*, siplan*, uint64_t, uint64_t);

#endif
