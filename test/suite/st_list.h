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

typedef enum {
	ST_SVV,
	ST_SV
} stlisttype;

struct stlist {
	stlisttype type;
	ssbuf list;
};

static inline void
st_listinit(stlist *l, stlisttype type)
{
	l->type = type;
	ss_bufinit(&l->list);
}

static inline void
st_listfree(stlist *l, sr *r)
{
	ssiter i;
	ss_iterinit(ss_bufiterref, &i);
	switch (l->type) {
	case ST_SVV:
		ss_iteropen(ss_bufiterref, &i, &l->list, sizeof(svv*));
		while (ss_iteratorhas(&i)) {
			svv *v = (svv*)ss_iteratorof(&i);
			sv_vunref(r, v);
			ss_iteratornext(&i);
		}
		break;
	case ST_SV:
		ss_iteropen(ss_bufiterref, &i, &l->list, sizeof(sv*));
		while (ss_iteratorhas(&i)) {
			sv *v = (sv*)ss_iteratorof(&i);
			sv_vunref(r, v->v);
			ss_free(r->a, v);
			ss_iteratornext(&i);
		}
		break;
	}
	ss_buffree(&l->list, r->a);
}

#endif
