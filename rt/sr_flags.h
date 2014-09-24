#ifndef SR_FLAGS_H_
#define SR_FLAGS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srflags srflags;

struct srflags {
	srspinlock lock;
	volatile uint32_t flags;
};

static inline void
sr_flagsinit(srflags *f, uint32_t init)
{
	sr_spinlockinit(&f->lock);
	f->flags = init;
}

static inline void
sr_flagsfree(srflags *f)
{
	sr_spinlockfree(&f->lock);
}

static inline uint32_t
sr_flagsof(srflags *f)
{
	sr_spinlock(&f->lock);
	uint32_t v = f->flags;
	sr_spinunlock(&f->lock);
	return v;
}

static inline int
sr_flagsisset(srflags *f, uint32_t v)
{
	sr_spinlock(&f->lock);
	int rc = (f->flags & v) > 0;
	sr_spinunlock(&f->lock);
	return rc;
}

static inline void
sr_flagsset(srflags *f, uint32_t v)
{
	sr_spinlock(&f->lock);
	f->flags |= v;
	sr_spinunlock(&f->lock);
}

static inline int
sr_flagstryset(srflags *f, uint32_t v)
{
	sr_spinlock(&f->lock);
	if (srunlikely(f->flags & v)) {
		sr_spinunlock(&f->lock);
		return 1;
	}
	f->flags |= v;
	sr_spinunlock(&f->lock);
	return 0;
}

static inline int
sr_flagstryset_if(srflags *f, uint32_t cond, uint32_t v)
{
	sr_spinlock(&f->lock);
	if (srunlikely(f->flags == cond)) {
		sr_spinunlock(&f->lock);
		return 1;
	}
	f->flags |= v;
	sr_spinunlock(&f->lock);
	return 0;
}

static inline void
sr_flagsunset(srflags *f, uint32_t v)
{
	sr_spinlock(&f->lock);
	f->flags &= ~v;
	sr_spinunlock(&f->lock);
}

#endif
