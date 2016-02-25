#ifndef SO_POOL_H_
#define SO_POOL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sopool sopool;

struct sopool {
	ssspinlock lock;
	int free_max;
	solist list;
	solist free;
};

static inline void
so_poolinit(sopool *p, int n)
{
	ss_spinlockinit(&p->lock);
	so_listinit(&p->list);
	so_listinit(&p->free);
	p->free_max = n;
}

static inline int
so_pooldestroy(sopool *p)
{
	ss_spinlockfree(&p->lock);
	int rcret = 0;
	int rc = so_listdestroy(&p->list);
	if (ssunlikely(rc == -1))
		rcret = -1;
	so_listfree(&p->free);
	return rcret;
}

static inline void
so_pooladd(sopool *p, so *o)
{
	ss_spinlock(&p->lock);
	so_listadd(&p->list, o);
	ss_spinunlock(&p->lock);
}

static inline void
so_poolgc(sopool *p, so *o)
{
	ss_spinlock(&p->lock);
	so_listdel(&p->list, o);
	if (p->free.n < p->free_max) {
		so_listadd(&p->free, o);
		ss_spinunlock(&p->lock);
		return;
	}
	ss_spinunlock(&p->lock);
	so_free(o);
}

static inline void
so_poolpush(sopool *p, so *o)
{
	ss_spinlock(&p->lock);
	so_listadd(&p->free, o);
	ss_spinunlock(&p->lock);
}

static inline so*
so_poolpop(sopool *p)
{
	so *o = NULL;
	ss_spinlock(&p->lock);
	if (sslikely(p->free.n)) {
		o = so_listfirst(&p->free);
		so_listdel(&p->free, o);
	}
	ss_spinunlock(&p->lock);
	return o;
}

#endif
