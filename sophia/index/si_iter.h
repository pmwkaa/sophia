#ifndef SI_ITER_H_
#define SI_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siiter siiter;

struct siiter {
	si *index;
	ssrbnode *v;
	ssorder order;
	char *key;
} sspacked;

ss_rbget(si_itermatch,
         si_nodecmp(sscast(n, sinode, node), key, scheme))

static inline int
si_iter_open(ssiter *i, sr *r, si *index, ssorder o, char *key)
{
	siiter *ii = (siiter*)i->priv;
	ii->index = index;
	ii->order = o;
	ii->key   = key;
	ii->v     = NULL;
	int eq = 0;
	if (ssunlikely(ii->index->n == 1)) {
		ii->v = ss_rbmin(&ii->index->i);
		return 1;
	}
	if (ssunlikely(ii->key == NULL)) {
		switch (ii->order) {
		case SS_LT:
		case SS_LTE:
			ii->v = ss_rbmax(&ii->index->i);
			break;
		case SS_GT:
		case SS_GTE:
			ii->v = ss_rbmin(&ii->index->i);
			break;
		default:
			assert(0);
			break;
		}
		return 0;
	}
	/* route */
	assert(ii->key != NULL);
	int rc;
	rc = si_itermatch(&ii->index->i, r->scheme, ii->key, 0, &ii->v);
	if (ssunlikely(ii->v == NULL)) {
		assert(rc != 0);
		if (rc == 1)
			ii->v = ss_rbmin(&ii->index->i);
		else
			ii->v = ss_rbmax(&ii->index->i);
	} else {
		eq = rc == 0 && ii->v;
		if (rc == 1) {
			ii->v = ss_rbprev(&ii->index->i, ii->v);
			if (ssunlikely(ii->v == NULL))
				ii->v = ss_rbmin(&ii->index->i);
		}
	}
	assert(ii->v != NULL);
	return eq;
}

static inline void
si_iter_close(ssiter *i ssunused)
{ }

static inline int
si_iter_has(ssiter *i)
{
	siiter *ii = (siiter*)i->priv;
	return ii->v != NULL;
}

static inline void*
si_iter_of(ssiter *i)
{
	siiter *ii = (siiter*)i->priv;
	if (ssunlikely(ii->v == NULL))
		return NULL;
	sinode *n = si_nodeof(ii->v);
	return n;
}

static inline void
si_iter_next(ssiter *i)
{
	siiter *ii = (siiter*)i->priv;
	switch (ii->order) {
	case SS_LT:
	case SS_LTE:
		ii->v = ss_rbprev(&ii->index->i, ii->v);
		break;
	case SS_GT:
	case SS_GTE:
		ii->v = ss_rbnext(&ii->index->i, ii->v);
		break;
	default: assert(0);
	}
}

extern ssiterif si_iter;

#endif
