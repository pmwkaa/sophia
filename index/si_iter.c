
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

sr_rbget(si_itermatch,
         si_nodecmp(srcast(n, sinode, node), key, keysize, cmp))

typedef struct siiter siiter;

struct siiter {
	si *index;
	srrbnode *v;
	srorder order;
	void *key;
	int keysize;
} srpacked;

static void
si_iterinit(sriter *i)
{
	assert(sizeof(siiter) <= sizeof(i->priv));
	siiter *ii = (siiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static int
si_iteropen(sriter *i, va_list args)
{
	siiter *ii = (siiter*)i->priv;
	ii->index   = va_arg(args, si*);
	ii->order   = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->v       = NULL;
	int eq = 0;
	if (srunlikely(ii->index->n == 1)) {
		ii->v = sr_rbmin(&ii->index->i);
		return 1;
	}
	int rc;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		if (srunlikely(ii->key == NULL)) {
			ii->v = sr_rbmax(&ii->index->i);
			break;
		}
		rc = si_itermatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case 0:
			if (ii->order == SR_LT) {
				eq = 1;
				sinode *n = si_nodeof(ii->v);
				sdindexpage *min = sd_indexmin(&n->index);
				int l = sr_compare(i->r->cmp, sd_indexpage_min(min), min->sizemin,
								   ii->key, ii->keysize);
				if (srunlikely(l == 0))
					ii->v = sr_rbprev(&ii->index->i, ii->v);
			}
			break;
		case 1:
			ii->v = sr_rbprev(&ii->index->i, ii->v);
			break;
		}
		break;
	case SR_GT:
	case SR_GTE:
		if (srunlikely(ii->key == NULL)) {
			ii->v = sr_rbmin(&ii->index->i);
			break;
		}
		rc = si_itermatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case  0:
			if (ii->order == SR_GT) {
				eq = 1;
				sinode *n = si_nodeof(ii->v);
				sdindexpage *max = sd_indexmax(&n->index);
				int r = sr_compare(i->r->cmp, sd_indexpage_max(max), max->sizemax,
								   ii->key, ii->keysize);
				if (srunlikely(r == 0))
					ii->v = sr_rbnext(&ii->index->i, ii->v);
			}
			break;
		case -1:
			ii->v = sr_rbnext(&ii->index->i, ii->v);
			break;
		}
		break;
	case SR_ROUTE:
		assert(ii->key != NULL);
		rc = si_itermatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
		if (srunlikely(ii->v == NULL)) {
			assert(rc != 0);
			if (rc == 1)
				ii->v = sr_rbmin(&ii->index->i);
			else
				ii->v = sr_rbmax(&ii->index->i);
		} else {
			eq = rc == 0 && ii->v;
			if (rc == 1) {
				ii->v = sr_rbprev(&ii->index->i, ii->v);
				if (srunlikely(ii->v == NULL))
					ii->v = sr_rbmin(&ii->index->i);
			}
		}
		assert(ii->v != NULL);
		break;
	case SR_RANDOM: {
		assert(ii->key != NULL);
		uint32_t rnd = *(uint32_t*)ii->key;
		rnd %= ii->index->n;
		ii->v = sr_rbmin(&ii->index->i);
		uint32_t pos = 0;
		while (pos != rnd) {
			ii->v = sr_rbnext(&ii->index->i, ii->v);
			pos++;
		}
		break;
	}
	default: assert(0);
	}
	return eq;
}

static void
si_iterclose(sriter *i srunused)
{ }

static int
si_iterhas(sriter *i)
{
	siiter *ii = (siiter*)i->priv;
	return ii->v != NULL;
}

static void*
si_iterof(sriter *i)
{
	siiter *ii = (siiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	sinode *n = si_nodeof(ii->v);
	return n;
}

static void
si_iternext(sriter *i)
{
	siiter *ii = (siiter*)i->priv;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		ii->v = sr_rbprev(&ii->index->i, ii->v);
		break;
	case SR_GT:
	case SR_GTE:
		ii->v = sr_rbnext(&ii->index->i, ii->v);
		break;
	default: assert(0);
	}
}

sriterif si_iter =
{
	.init  = si_iterinit,
	.open  = si_iteropen,
	.close = si_iterclose,
	.has   = si_iterhas,
	.of    = si_iterof,
	.next  = si_iternext
};
