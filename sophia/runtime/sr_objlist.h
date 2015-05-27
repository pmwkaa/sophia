#ifndef SR_OBJLIST_H_
#define SR_OBJLIST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srobjlist srobjlist;

struct srobjlist {
	sslist list;
	int n;
};

static inline void
sr_objlist_init(srobjlist *i)
{
	ss_listinit(&i->list);
	i->n = 0;
}

static inline int
sr_objlist_destroy(srobjlist *i)
{
	int rcret = 0;
	int rc;
	sslist *p, *n;
	ss_listforeach_safe(&i->list, p, n) {
		srobj *o = sscast(p, srobj, link);
		rc = sr_objdestroy(o);
		if (ssunlikely(rc == -1))
			rcret = -1;
	}
	i->n = 0;
	ss_listinit(&i->list);
	return rcret;
}

static inline void
sr_objlist_add(srobjlist *i, srobj *o)
{
	ss_listappend(&i->list, &o->link);
	i->n++;
}

static inline void
sr_objlist_del(srobjlist *i, srobj *o)
{
	ss_listunlink(&o->link);
	i->n--;
}

static inline srobj*
sr_objlist_first(srobjlist *i)
{
	assert(i->n > 0);
	return sscast(i->list.next, srobj, link);
}

#endif
