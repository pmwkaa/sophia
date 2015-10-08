#ifndef ST_LIST_H_
#define ST_LIST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stlist stlist;

struct stlist {
	int svv;
	ssbuf list;
};

static inline void
st_listinit(stlist *l, int svv)
{
	l->svv = svv;
	ss_bufinit(&l->list);
}

static inline void
st_listfree(stlist *l, sr *r)
{
	ssiter i;
	ss_iterinit(ss_bufiterref, &i);
	if (l->svv) {
		ss_iteropen(ss_bufiterref, &i, &l->list, sizeof(svv*));
		while (ss_iteratorhas(&i)) {
			svv *v = (svv*)ss_iteratorof(&i);
			sv_vfree(r, v);
			ss_iteratornext(&i);
		}
	} else {
		ss_iteropen(ss_bufiterref, &i, &l->list, sizeof(sv*));
		while (ss_iteratorhas(&i)) {
			sv *v = (sv*)ss_iteratorof(&i);
			sv_vfree(r, v->v);
			ss_free(r->a, v);
			ss_iteratornext(&i);
		}
	}
	ss_buffree(&l->list, r->a);
}

#endif
