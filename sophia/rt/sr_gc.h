#ifndef SR_GC_H_
#define SR_GC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srgc srgc;

struct srgc {
	srspinlock lock;
	int mark;
	int sweep;
	int complete;
};

static inline void
sr_gcinit(srgc *gc)
{
	sr_spinlockinit(&gc->lock);
	gc->mark     = 0;
	gc->sweep    = 0;
	gc->complete = 0;
}

static inline void
sr_gclock(srgc *gc) {
	sr_spinlock(&gc->lock);
}

static inline void
sr_gcunlock(srgc *gc) {
	sr_spinunlock(&gc->lock);
}

static inline void
sr_gcfree(srgc *gc)
{
	sr_spinlockfree(&gc->lock);
}

static inline void
sr_gcmark(srgc *gc, int n)
{
	sr_spinlock(&gc->lock);
	gc->mark += n;
	sr_spinunlock(&gc->lock);
}

static inline void
sr_gcsweep(srgc *gc, int n)
{
	sr_spinlock(&gc->lock);
	gc->sweep += n;
	sr_spinunlock(&gc->lock);
}

static inline void
sr_gccomplete(srgc *gc)
{
	sr_spinlock(&gc->lock);
	gc->complete = 1;
	sr_spinunlock(&gc->lock);
}

static inline int
sr_gcinprogress(srgc *gc)
{
	sr_spinlock(&gc->lock);
	int v = gc->complete;
	sr_spinunlock(&gc->lock);
	return !v;
}

static inline int
sr_gcready(srgc *gc, float factor)
{
	sr_spinlock(&gc->lock);
	int ready = gc->sweep >= (gc->mark * factor);
	int rc = ready && gc->complete;
	sr_spinunlock(&gc->lock);
	return rc;
}

static inline int
sr_gcrotateready(srgc *gc, int wm)
{
	sr_spinlock(&gc->lock);
	int rc = gc->mark >= wm;
	sr_spinunlock(&gc->lock);
	return rc;
}

static inline int
sr_gcgarbage(srgc *gc)
{
	sr_spinlock(&gc->lock);
	int ready = (gc->mark == gc->sweep);
	int rc = gc->complete && ready;
	sr_spinunlock(&gc->lock);
	return rc;
}

#endif
