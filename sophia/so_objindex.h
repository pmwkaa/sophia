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

static inline int
so_objindex_destroy(soobjindex *i)
{
	int rcret = 0;
	int rc;
	srlist *p, *n;
	sr_listforeach_safe(&i->list, p, n) {
		soobj *o = srcast(p, soobj, link);
		rc = so_objdestroy(o);
		if (srunlikely(rc == -1))
			rcret = -1;
	}
	i->n = 0;
	sr_listinit(&i->list);
	return rcret;
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
