#ifndef SO_OBJINDEX_H_
#define SO_OBJINDEX_H_

/*
 * sophia database
 * sehia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soobjindex soobjindex;

struct soobjindex {
	srspinlock lock;
	srlist list;
	int n;
};

static inline void
so_objindex_init(soobjindex *i)
{
	sr_spinlockinit(&i->lock);
	sr_listinit(&i->list);
	i->n = 0;
}

static inline void
so_objindex_free(soobjindex *i)
{
	sr_spinlockfree(&i->lock);
	sr_listinit(&i->list);
	i->n = 0;
}

static inline void
so_objindex_lock(soobjindex *i) {
	sr_spinlock(&i->lock);
}

static inline void
so_objindex_unlock(soobjindex *i) {
	sr_spinunlock(&i->lock);
}

static inline void
so_objindex_register(soobjindex *i, soobj *o)
{
	so_objindex_lock(i);
	sr_listappend(&i->list, &o->olink);
	i->n++;
	so_objindex_unlock(i);
}

static inline void
so_objindex_unregister(soobjindex *i, soobj *o)
{
	so_objindex_lock(i);
	sr_listunlink(&o->olink);
	i->n--;
	so_objindex_unlock(i);
}

#endif
