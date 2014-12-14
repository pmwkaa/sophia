
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>

typedef struct sxiter sxiter;

struct sxiter {
	sxindex *index;
	srrbnode *v;
	sxv *vcur;
	sv current;
	srorder order;
	void *key;
	int keysize;
	uint32_t id;
} srpacked;

static void
sx_iterinit(sriter *i)
{
	assert(sizeof(sxiter) <= sizeof(i->priv));

	sxiter *ii = (sxiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline void
sx_iterfwd(sxiter *i)
{
	while (i->v) {
		sxv *v = srcast(i->v, sxv, node);
		i->vcur = sx_vmatch(v, i->id);
		if (srlikely(i->vcur))
			return;
		i->v = sr_rbnext(&i->index->i, i->v);
	}
}

static inline void
sx_iterbkw(sxiter *i)
{
	while (i->v) {
		sxv *v = srcast(i->v, sxv, node);
		i->vcur = sx_vmatch(v, i->id);
		if (srlikely(i->vcur))
			return;
		i->v = sr_rbprev(&i->index->i, i->v);
	}
}

sr_rbget(sx_itermatch,
         sr_compare(cmp, sv_vkey((srcast(n, sxv, node))->v),
                    (srcast(n, sxv, node))->v->keysize,
                    key, keysize))

static int
sx_iteropen(sriter *i, va_list args)
{
	sxiter *ii = (sxiter*)i->priv;
	ii->index   = va_arg(args, sxindex*);
	ii->order   = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->id      = va_arg(args, uint32_t);
	srrbnode *n = NULL;
	int eq = 0;
	int rc;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		if (srunlikely(ii->key == NULL)) {
			ii->v = sr_rbmax(&ii->index->i);
			sx_iterbkw(ii);
			break;
		}
		rc = sx_itermatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case 0:
			eq = 1;
			if (ii->order == SR_LT)
				ii->v = sr_rbprev(&ii->index->i, ii->v);
			break;
		case 1:
			ii->v = sr_rbprev(&ii->index->i, ii->v);
			break;
		}
		n = ii->v;
		sx_iterbkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		if (srunlikely(ii->key == NULL)) {
			ii->v = sr_rbmin(&ii->index->i);
			sx_iterfwd(ii);
			break;
		}
		rc = sx_itermatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case  0:
			eq = 1;
			if (ii->order == SR_GT)
				ii->v = sr_rbnext(&ii->index->i, ii->v);
			break;
		case -1:
			ii->v = sr_rbnext(&ii->index->i, ii->v);
			break;
		}
		n = ii->v;
		sx_iterfwd(ii);
		break;
	case SR_RANDOM:
		ii->v = NULL;
		break;
	default: assert(0);
	}
	return eq && (n == ii->v);
}

static void
sx_iterclose(sriter *i srunused)
{}

static int
sx_iterhas(sriter *i)
{
	sxiter *ii = (sxiter*)i->priv;
	return ii->v != NULL;
}

static void*
sx_iterof(sriter *i)
{
	sxiter *ii = (sxiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	assert(ii->vcur != NULL);
	svinit(&ii->current, &sx_vif, ii->vcur, NULL);
	return &ii->current;
}

static void
sx_iternext(sriter *i)
{
	sxiter *ii = (sxiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		ii->v = sr_rbprev(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sx_iterbkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		ii->v = sr_rbnext(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sx_iterfwd(ii);
		break;
	default: assert(0);
	}
}

sriterif sx_iter =
{
	.init    = sx_iterinit,
	.open    = sx_iteropen,
	.close   = sx_iterclose,
	.has     = sx_iterhas,
	.of      = sx_iterof,
	.next    = sx_iternext
};
