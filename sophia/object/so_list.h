#ifndef SO_LIST_H_
#define SO_LIST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct solist solist;

struct solist {
	sslist list;
	int n;
};

static inline void
so_listinit(solist *i)
{
	ss_listinit(&i->list);
	i->n = 0;
}

static inline int
so_listdestroy(solist *i, int fe)
{
	int rcret = 0;
	int rc;
	sslist *p, *n;
	ss_listforeach_safe(&i->list, p, n) {
		so *o = sscast(p, so, link);
		rc = so_destroy(o, fe);
		if (ssunlikely(rc == -1))
			rcret = -1;
	}
	i->n = 0;
	ss_listinit(&i->list);
	return rcret;
}

static inline void
so_listfree(solist *i)
{
	sslist *p, *n;
	ss_listforeach_safe(&i->list, p, n) {
		so *o = sscast(p, so, link);
		so_free(o);
	}
	i->n = 0;
	ss_listinit(&i->list);
}

static inline void
so_listadd(solist *i, so *o)
{
	ss_listappend(&i->list, &o->link);
	i->n++;
}

static inline void
so_listdel(solist *i, so *o)
{
	ss_listunlink(&o->link);
	i->n--;
}

static inline so*
so_listfirst(solist *i)
{
	assert(i->n > 0);
	return sscast(i->list.next, so, link);
}

#endif
