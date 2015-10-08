#ifndef SR_STAT_H_
#define SR_STAT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srstat srstat;

struct srstat {
	ssspinlock lock;
	uint64_t v_count;
	uint64_t v_allocated;
};

static inline void
sr_statinit(srstat *s)
{
	s->v_count = 0;
	s->v_allocated = 0;
	ss_spinlockinit(&s->lock);
}

static inline void
sr_statfree(srstat *s)
{
	ss_spinlockfree(&s->lock);
}

#endif
