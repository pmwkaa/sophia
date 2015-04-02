
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

typedef struct svindexiter svindexiter;

struct svindexiter {
	svindex *index;
	srrbnode *v;
	svv *vcur;
	sv current;
	srorder order;
	void *key;
	int keysize;
	uint64_t vlsn;
} srpacked;

static inline void
sv_indexiter_fwd(svindexiter *i)
{
	while (i->v) {
		svv *v = srcast(i->v, svv, node);
		i->vcur = sv_visible(v, i->vlsn);
		if (srlikely(i->vcur))
			return;
		i->v = sr_rbnext(&i->index->i, i->v);
	}
}

static inline void
sv_indexiter_bkw(svindexiter *i)
{
	while (i->v) {
		svv *v = srcast(i->v, svv, node);
		i->vcur = sv_visible(v, i->vlsn);
		if (srlikely(i->vcur))
			return;
		i->v = sr_rbprev(&i->index->i, i->v);
	}
}

static int
sv_indexiter_open(sriter *i, va_list args)
{
	svindexiter *ii = (svindexiter*)i->priv;
	ii->index   = va_arg(args, svindex*);
	ii->order   = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->vlsn    = va_arg(args, uint64_t);
	ii->v       = NULL;
	ii->vcur    = NULL;
	memset(&ii->current, 0, sizeof(ii->current));
	srrbnode *n = NULL;
	int eq = 0;
	int rc;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
	case SR_EQ:
		if (srunlikely(ii->key == NULL)) {
			ii->v = sr_rbmax(&ii->index->i);
			sv_indexiter_bkw(ii);
			break;
		}
		rc = sv_indexmatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
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
		sv_indexiter_bkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		if (srunlikely(ii->key == NULL)) {
			ii->v = sr_rbmin(&ii->index->i);
			sv_indexiter_fwd(ii);
			break;
		}
		rc = sv_indexmatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
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
		sv_indexiter_fwd(ii);
		break;
	case SR_RANDOM: {
		assert(ii->key != NULL);
		if (srunlikely(ii->index->count == 0)) {
			ii->v = NULL;
			ii->vcur = NULL;
			break;
		}
		uint32_t rnd = *(uint32_t*)ii->key;
		rnd %= ii->index->count;
		ii->v = sr_rbmin(&ii->index->i);
		uint32_t pos = 0;
		while (pos != rnd) {
			ii->v = sr_rbnext(&ii->index->i, ii->v);
			pos++;
		}
		svv *v = srcast(ii->v, svv, node);
		ii->vcur = v;
		break;
	}
	case SR_UPDATE:
		rc = sv_indexmatch(&ii->index->i, i->r->cmp, ii->key, ii->keysize, &ii->v);
		if (rc == 0 && ii->v) {
			svv *v = srcast(ii->v, svv, node);
			ii->vcur = v;
			return v->lsn > ii->vlsn;
		}
		return 0;
	default: assert(0);
	}
	return eq && (n == ii->v);
}

static void
sv_indexiter_close(sriter *i srunused)
{}

static int
sv_indexiter_has(sriter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	return ii->v != NULL;
}

static void*
sv_indexiter_of(sriter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	assert(ii->vcur != NULL);
	sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	return &ii->current;
}

static void
sv_indexiter_next(sriter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		ii->v = sr_rbprev(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sv_indexiter_bkw(ii);
		break;
	case SR_RANDOM:
	case SR_GT:
	case SR_GTE:
		ii->v = sr_rbnext(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sv_indexiter_fwd(ii);
		break;
	default: assert(0);
	}
}

sriterif sv_indexiter =
{
	.open    = sv_indexiter_open,
	.close   = sv_indexiter_close,
	.has     = sv_indexiter_has,
	.of      = sv_indexiter_of,
	.next    = sv_indexiter_next
};

typedef struct svindexiterraw svindexiterraw;

struct svindexiterraw {
	svindex *index;
	srrbnode *v;
	svv *vcur;
	sv current;
} srpacked;

static int
sv_indexiterraw_open(sriter *i, va_list args)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	ii->index = va_arg(args, svindex*);
	ii->v = sr_rbmin(&ii->index->i);
	ii->vcur = NULL;
	if (ii->v) {
		ii->vcur = srcast(ii->v, svv, node);
	}
	return 0;
}

static void
sv_indexiterraw_close(sriter *i srunused)
{}

static int
sv_indexiterraw_has(sriter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	return ii->v != NULL;
}

static void*
sv_indexiterraw_of(sriter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	assert(ii->vcur != NULL);
	sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	return &ii->current;
}

static void
sv_indexiterraw_next(sriter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	if (srunlikely(ii->v == NULL))
		return;
	assert(ii->vcur != NULL);
	svv *v = ii->vcur->next;
	if (v) {
		ii->vcur = v;
		return;
	}
	ii->v = sr_rbnext(&ii->index->i, ii->v);
	ii->vcur = NULL;
	if (ii->v) {
		ii->vcur = srcast(ii->v, svv, node);
	}
}

sriterif sv_indexiterraw =
{
	.open    = sv_indexiterraw_open,
	.close   = sv_indexiterraw_close,
	.has     = sv_indexiterraw_has,
	.of      = sv_indexiterraw_of,
	.next    = sv_indexiterraw_next
};
