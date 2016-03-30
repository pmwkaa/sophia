#ifndef SC_STEP_H_
#define SC_STEP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline void
sc_task_checkpoint(sc *s)
{
	uint64_t lsn = sr_seq(s->r->seq, SR_LSN);
	s->checkpoint_lsn = lsn;
	s->checkpoint = 1;
	sc_start(s, SI_CHECKPOINT);
}

static inline void
sc_task_checkpoint_done(sc *s)
{
	s->checkpoint = 0;
	s->checkpoint_lsn_last = s->checkpoint_lsn;
	s->checkpoint_lsn = 0;
}

static inline void
sc_task_anticache(sc *s)
{
	s->anticache = 1;
	s->anticache_storage = s->anticache_limit;
	s->anticache_asn = sr_seq(s->r->seq, SR_ASNNEXT);
	sc_start(s, SI_ANTICACHE);
}

static inline void
sc_task_anticache_done(sc *s, uint64_t now)
{
	s->anticache = 0;
	s->anticache_asn_last = s->anticache_asn;
	s->anticache_asn = 0;
	s->anticache_storage = 0;
	s->anticache_time = now;
}

static inline void
sc_task_snapshot(sc *s)
{
	s->snapshot = 1;
	s->snapshot_ssn = sr_seq(s->r->seq, SR_SSNNEXT);
	sc_start(s, SI_SNAPSHOT);
}

static inline void
sc_task_snapshot_done(sc *s, uint64_t now)
{
	s->snapshot = 0;
	s->snapshot_ssn_last = s->snapshot_ssn;
	s->snapshot_ssn = 0;
	s->snapshot_time = now;
}

static inline void
sc_task_expire(sc *s)
{
	s->expire = 1;
	sc_start(s, SI_EXPIRE);
}

static inline void
sc_task_expire_done(sc *s, uint64_t now)
{
	s->expire = 0;
	s->expire_time = now;
}

static inline void
sc_task_gc(sc *s)
{
	s->gc = 1;
	sc_start(s, SI_GC);
}

static inline void
sc_task_gc_done(sc *s, uint64_t now)
{
	s->gc = 0;
	s->gc_time = now;
}

static inline void
sc_task_lru(sc *s)
{
	s->lru = 1;
	sc_start(s, SI_LRU);
}

static inline void
sc_task_lru_done(sc *s, uint64_t now)
{
	s->lru = 0;
	s->lru_time = now;
}

static inline void
sc_task_age(sc *s)
{
	s->age = 1;
	sc_start(s, SI_AGE);
}

static inline void
sc_task_age_done(sc *s, uint64_t now)
{
	s->age = 0;
	s->age_time = now;
}

int sc_step(sc*, scworker*, uint64_t);

#endif
