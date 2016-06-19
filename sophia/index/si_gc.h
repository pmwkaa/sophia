#ifndef SI_GC_H_
#define SI_GC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

void si_gcv(sr*, svv*);

static inline void
si_gcvall(sr *r, svv *v)
{
	while (v) {
		svv *n = v->next;
		si_gcv(r, v);
		v = n;
	}
}

#endif
