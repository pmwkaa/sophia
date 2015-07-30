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
	ssrbnode *v;
	svv *vcur;
	sv current;
	ssorder order;
} sspacked;

static inline int
sv_indexiter_open(ssiter *i, sr *r, svindex *index, ssorder o, void *key, int keysize)
{
	svindexiter *ii = (svindexiter*)i->priv;
	ii->index   = index;
	ii->order   = o;
	ii->v       = NULL;
	ii->vcur    = NULL;
	memset(&ii->current, 0, sizeof(ii->current));
	int rc;
	int eq = 0;
	switch (ii->order) {
	case SS_LT:
	case SS_LTE:
		if (ssunlikely(key == NULL)) {
			ii->v = ss_rbmax(&ii->index->i);
			break;
		}
		rc = sv_indexmatch(&ii->index->i, r->scheme, key, keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case 0:
			eq = 1;
			if (ii->order == SS_LT)
				ii->v = ss_rbprev(&ii->index->i, ii->v);
			break;
		case 1:
			ii->v = ss_rbprev(&ii->index->i, ii->v);
			break;
		}
		break;
	case SS_GT:
	case SS_GTE:
		if (ssunlikely(key == NULL)) {
			ii->v = ss_rbmin(&ii->index->i);
			break;
		}
		rc = sv_indexmatch(&ii->index->i, r->scheme, key, keysize, &ii->v);
		if (ii->v == NULL)
			break;
		switch (rc) {
		case  0:
			eq = 1;
			if (ii->order == SS_GT)
				ii->v = ss_rbnext(&ii->index->i, ii->v);
			break;
		case -1:
			ii->v = ss_rbnext(&ii->index->i, ii->v);
			break;
		}
		break;
	default: assert(0);
	}
	ii->vcur = NULL;
	if (ii->v) {
		ii->vcur = sscast(ii->v, svv, node);
		sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	}
	return eq;
}

static inline void
sv_indexiter_close(ssiter *i ssunused)
{}

static inline int
sv_indexiter_has(ssiter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	return ii->v != NULL;
}

static inline void*
sv_indexiter_of(ssiter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	if (ssunlikely(ii->v == NULL))
		return NULL;
	return &ii->current;
}

static inline void
sv_indexiter_next(ssiter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	if (ssunlikely(ii->v == NULL))
		return;
	assert(ii->vcur != NULL);
	svv *v = ii->vcur->next;
	if (v) {
		ii->vcur = v;
		sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
		return;
	}
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
	ii->vcur = NULL;
	if (ii->v) {
		ii->vcur = sscast(ii->v, svv, node);
		sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	}
}

extern ssiterif sv_indexiter;

#endif
