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
	srlist list;
	int n;
};

static inline void
so_objindex_init(soobjindex *i)
{
	sr_listinit(&i->list);
	i->n = 0;
}

static inline void
so_objindex_free(soobjindex *i)
{
	sr_listinit(&i->list);
	i->n = 0;
}

static inline void
so_objindex_register(soobjindex *i, soobj *o)
{
	sr_listappend(&i->list, &o->link);
	i->n++;
}

static inline void
so_objindex_unregister(soobjindex *i, soobj *o)
{
	sr_listunlink(&o->link);
	i->n--;
}

#endif
