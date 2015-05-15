#ifndef SV_INDEXITER_H_
#define SV_INDEXITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

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

static inline int
sv_indexiter_open(sriter *i, svindex *index, srorder o, void *key, int keysize, uint64_t vlsn)
{
	svindexiter *ii = (svindexiter*)i->priv;
	ii->index   = index;
	ii->order   = o;
	ii->key     = key;
	ii->keysize = keysize;
	ii->vlsn    = vlsn;
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
		rc = sv_indexmatch(&ii->index->i, i->r->scheme, ii->key, ii->keysize, &ii->v);
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
		rc = sv_indexmatch(&ii->index->i, i->r->scheme, ii->key, ii->keysize, &ii->v);
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
	case SR_UPDATE:
		rc = sv_indexmatch(&ii->index->i, i->r->scheme, ii->key, ii->keysize, &ii->v);
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

static inline void
sv_indexiter_close(sriter *i srunused)
{}

static inline int
sv_indexiter_has(sriter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	return ii->v != NULL;
}

static inline void*
sv_indexiter_of(sriter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	assert(ii->vcur != NULL);
	sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	return &ii->current;
}

static inline void
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
	case SR_GT:
	case SR_GTE:
		ii->v = sr_rbnext(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sv_indexiter_fwd(ii);
		break;
	default: assert(0);
	}
}

typedef struct svindexiterraw svindexiterraw;

struct svindexiterraw {
	svindex *index;
	srrbnode *v;
	svv *vcur;
	sv current;
} srpacked;

static inline int
sv_indexiterraw_open(sriter *i, svindex *index)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	ii->index = index;
	ii->v = sr_rbmin(&ii->index->i);
	ii->vcur = NULL;
	if (ii->v) {
		ii->vcur = srcast(ii->v, svv, node);
	}
	return 0;
}

static inline void
sv_indexiterraw_close(sriter *i srunused)
{}

static inline int
sv_indexiterraw_has(sriter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	return ii->v != NULL;
}

static inline void*
sv_indexiterraw_of(sriter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	assert(ii->vcur != NULL);
	sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	return &ii->current;
}

static inline void
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

extern sriterif sv_indexiter;
extern sriterif sv_indexiterraw;

#endif
