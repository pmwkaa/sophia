#ifndef SS_GC_H_
#define SS_GC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssgc ssgc;

struct ssgc {
	ssspinlock lock;
	int mark;
	int sweep;
	int complete;
};

static inline void
ss_gcinit(ssgc *gc)
{
	ss_spinlockinit(&gc->lock);
	gc->mark     = 0;
	gc->sweep    = 0;
	gc->complete = 0;
}

static inline void
ss_gclock(ssgc *gc) {
	ss_spinlock(&gc->lock);
}

static inline void
ss_gcunlock(ssgc *gc) {
	ss_spinunlock(&gc->lock);
}

static inline void
ss_gcfree(ssgc *gc)
{
	ss_spinlockfree(&gc->lock);
}

static inline void
ss_gcmark(ssgc *gc, int n)
{
	ss_spinlock(&gc->lock);
	gc->mark += n;
	ss_spinunlock(&gc->lock);
}

static inline void
ss_gcsweep(ssgc *gc, int n)
{
	ss_spinlock(&gc->lock);
	gc->sweep += n;
	ss_spinunlock(&gc->lock);
}

static inline void
ss_gccomplete(ssgc *gc)
{
	ss_spinlock(&gc->lock);
	gc->complete = 1;
	ss_spinunlock(&gc->lock);
}

static inline int
ss_gcinprogress(ssgc *gc)
{
	ss_spinlock(&gc->lock);
	int v = gc->complete;
	ss_spinunlock(&gc->lock);
	return !v;
}

static inline int
ss_gcready(ssgc *gc, float factor)
{
	ss_spinlock(&gc->lock);
	int ready = gc->sweep >= (gc->mark * factor);
	int rc = ready && gc->complete;
	ss_spinunlock(&gc->lock);
	return rc;
}

static inline int
ss_gcrotateready(ssgc *gc, int wm)
{
	ss_spinlock(&gc->lock);
	int rc = gc->mark >= wm;
	ss_spinunlock(&gc->lock);
	return rc;
}

static inline int
ss_gcgarbage(ssgc *gc)
{
	ss_spinlock(&gc->lock);
	int ready = (gc->mark == gc->sweep);
	int rc = gc->complete && ready;
	ss_spinunlock(&gc->lock);
	return rc;
}

#endif
