#ifndef SI_LRU_H_
#define SI_LRU_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline void
si_lru_add(si *i, svref *ref)
{
	i->lru_intr_sum += ref->v->size;
	if (ssunlikely(i->lru_intr_sum >= i->scheme.lru_step))
	{
		uint64_t lsn = sr_seq(i->r.seq, SR_LSN);
		i->lru_v += (lsn - i->lru_intr_lsn);
		i->lru_steps++;
		i->lru_intr_lsn = lsn;
		i->lru_intr_sum = 0;
	}
}

static inline uint64_t
si_lru_vlsn_of(si *i)
{
	assert(i->scheme.lru_step != 0);
	uint64_t size = i->size;
	if (sslikely(size <= i->scheme.lru))
		return 0;
	uint64_t lru_v = i->lru_v;
	uint64_t lru_steps = i->lru_steps;
	uint64_t lru_avg_step;
	uint64_t oversize = size - i->scheme.lru;
	uint64_t steps = 1 + oversize / i->scheme.lru_step;
	lru_avg_step = lru_v / lru_steps;
	return i->lru_intr_lsn + (steps * lru_avg_step);
}

static inline uint64_t
si_lru_vlsn(si *i)
{
	if (sslikely(i->scheme.lru == 0))
		return 0;
	si_lock(i);
	int rc = si_lru_vlsn_of(i);
	si_unlock(i);
	return rc;
}

#endif
