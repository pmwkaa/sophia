
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>

typedef struct smiter smiter;

struct smiter {
	sm *index;
	srrbnode *v;
	smv *vcur;
	sv current;
	srorder order;
	void *key;
	int keysize;
	uint32_t id;
} srpacked;

static void
sm_iterinit(sriter *i)
{
	assert(sizeof(smiter) <= sizeof(i->priv));

	smiter *ii = (smiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline void
sm_iterfwd(smiter *i)
{
	while (i->v) {
		smv *v = srcast(i->v, smv, node);
		i->vcur = sm_vmatch(v, i->id);
		if (srlikely(i->vcur))
			return;
		i->v = sr_rbnext(&i->index->i, i->v);
	}
}

static inline void
sm_iterbkw(smiter *i)
{
	while (i->v) {
		smv *v = srcast(i->v, smv, node);
		i->vcur = sm_vmatch(v, i->id);
		if (srlikely(i->vcur))
			return;
		i->v = sr_rbprev(&i->index->i, i->v);
	}
}

sr_rbget(sm_itermatch,
         sr_compare(cmp, sv_vkey((srcast(n, smv, node))->v),
                    (srcast(n, smv, node))->v->keysize,
                    key, keysize))

static int
sm_iteropen(sriter *i, va_list args)
{
	smiter *ii = (smiter*)i->priv;
	ii->index   = va_arg(args, sm*);
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
			sm_iterbkw(ii);
			break;
		}
		rc = sm_itermatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
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
		sm_iterbkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		if (srunlikely(ii->key == NULL)) {
			ii->v = sr_rbmin(&ii->index->i);
			sm_iterfwd(ii);
			break;
		}
		rc = sm_itermatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
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
		sm_iterfwd(ii);
		break;
	case SR_RANDOM:
		ii->v = NULL;
		break;
	default: assert(0);
	}
	return eq && (n == ii->v);
}

static void
sm_iterclose(sriter *i srunused)
{}

static int
sm_iterhas(sriter *i)
{
	smiter *ii = (smiter*)i->priv;
	return ii->v != NULL;
}

static void*
sm_iterof(sriter *i)
{
	smiter *ii = (smiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	assert(ii->vcur != NULL);
	svinit(&ii->current, &sm_vif, ii->vcur, NULL);
	return &ii->current;
}

static void
sm_iternext(sriter *i)
{
	smiter *ii = (smiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		ii->v = sr_rbprev(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sm_iterbkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		ii->v = sr_rbnext(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sm_iterfwd(ii);
		break;
	default: assert(0);
	}
}

sriterif sm_iter =
{
	.init    = sm_iterinit,
	.open    = sm_iteropen,
	.close   = sm_iterclose,
	.has     = sm_iterhas,
	.of      = sm_iterof,
	.next    = sm_iternext
};
