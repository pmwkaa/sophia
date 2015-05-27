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
	void *key;
	int keysize;
} sspacked;

ss_rbget(si_itermatch,
         si_nodecmp(sscast(n, sinode, node), key, keysize, scheme))

static inline int
si_iter_open(ssiter *i, sr *r, si *index, ssorder o, void *key, int keysize)
{
	siiter *ii = (siiter*)i->priv;
	ii->index   = index;
	ii->order   = o;
	ii->key     = key;
	ii->keysize = keysize;
	ii->v       = NULL;
	int eq = 0;
	if (ssunlikely(ii->index->n == 1)) {
		ii->v = ss_rbmin(&ii->index->i);
		return 1;
	}
	int rc;
	switch (ii->order) {
	case SS_LT:
	case SS_LTE:
		if (ssunlikely(ii->key == NULL)) {
			ii->v = ss_rbmax(&ii->index->i);
			break;
		}
		rc = si_itermatch(&ii->index->i, r->scheme, ii->key, ii->keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case 0:
			if (ii->order == SS_LT) {
				eq = 1;
				sinode *n = si_nodeof(ii->v);
				sdindexpage *min = sd_indexmin(&n->self.index);
				rc = sr_compare(r->scheme,
				                sd_indexpage_min(&n->self.index, min),
				                min->sizemin,
				                ii->key, ii->keysize);
				if (ssunlikely(rc == 0))
					ii->v = ss_rbprev(&ii->index->i, ii->v);
			}
			break;
		case 1:
			ii->v = ss_rbprev(&ii->index->i, ii->v);
			break;
		}
		break;
	case SS_GT:
	case SS_GTE:
		if (ssunlikely(ii->key == NULL)) {
			ii->v = ss_rbmin(&ii->index->i);
			break;
		}
		rc = si_itermatch(&ii->index->i, r->scheme, ii->key, ii->keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case  0:
			if (ii->order == SS_GT) {
				eq = 1;
				sinode *n = si_nodeof(ii->v);
				sdindexpage *max = sd_indexmax(&n->self.index);
				rc = sr_compare(r->scheme,
				                sd_indexpage_max(&n->self.index, max),
				                max->sizemax,
				                ii->key, ii->keysize);
				if (ssunlikely(rc == 0))
					ii->v = ss_rbnext(&ii->index->i, ii->v);
			}
			break;
		case -1:
			ii->v = ss_rbnext(&ii->index->i, ii->v);
			break;
		}
		break;
	case SS_ROUTE:
		assert(ii->key != NULL);
		rc = si_itermatch(&ii->index->i, r->scheme, ii->key, ii->keysize, &ii->v);
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
		break;
	default: assert(0);
	}
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
