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
	void *key;
	int keysize;
	uint64_t vlsn;
	sr *r;
} sspacked;

static inline void
sv_indexiter_fwd(svindexiter *i)
{
	while (i->v) {
		svv *v = sscast(i->v, svv, node);
		i->vcur = sv_visible(v, i->vlsn);
		if (sslikely(i->vcur))
			return;
		i->v = ss_rbnext(&i->index->i, i->v);
	}
}

static inline void
sv_indexiter_bkw(svindexiter *i)
{
	while (i->v) {
		svv *v = sscast(i->v, svv, node);
		i->vcur = sv_visible(v, i->vlsn);
		if (sslikely(i->vcur))
			return;
		i->v = ss_rbprev(&i->index->i, i->v);
	}
}

static inline int
sv_indexiter_open(ssiter *i, sr *r, svindex *index, ssorder o, void *key, int keysize, uint64_t vlsn)
{
	svindexiter *ii = (svindexiter*)i->priv;
	ii->r       = r;
	ii->index   = index;
	ii->order   = o;
	ii->key     = key;
	ii->keysize = keysize;
	ii->vlsn    = vlsn;
	ii->v       = NULL;
	ii->vcur    = NULL;
	memset(&ii->current, 0, sizeof(ii->current));
	ssrbnode *n = NULL;
	int eq = 0;
	int rc;
	switch (ii->order) {
	case SS_LT:
	case SS_LTE:
	case SS_EQ:
		if (ssunlikely(ii->key == NULL)) {
			ii->v = ss_rbmax(&ii->index->i);
			sv_indexiter_bkw(ii);
			break;
		}
		rc = sv_indexmatch(&ii->index->i, ii->r->scheme, ii->key, ii->keysize, &ii->v);
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
		n = ii->v;
		sv_indexiter_bkw(ii);
		break;
	case SS_GT:
	case SS_GTE:
		if (ssunlikely(ii->key == NULL)) {
			ii->v = ss_rbmin(&ii->index->i);
			sv_indexiter_fwd(ii);
			break;
		}
		rc = sv_indexmatch(&ii->index->i, ii->r->scheme, ii->key, ii->keysize, &ii->v);
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
		n = ii->v;
		sv_indexiter_fwd(ii);
		break;
	case SS_UPDATE:
		rc = sv_indexmatch(&ii->index->i, ii->r->scheme, ii->key, ii->keysize, &ii->v);
		if (rc == 0 && ii->v) {
			svv *v = sscast(ii->v, svv, node);
			ii->vcur = v;
			return v->lsn > ii->vlsn;
		}
		return 0;
	default: assert(0);
	}
	return eq && (n == ii->v);
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
	assert(ii->vcur != NULL);
	sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	return &ii->current;
}

static inline void
sv_indexiter_next(ssiter *i)
{
	svindexiter *ii = (svindexiter*)i->priv;
	if (ssunlikely(ii->v == NULL))
		return;
	switch (ii->order) {
	case SS_LT:
	case SS_LTE:
		ii->v = ss_rbprev(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sv_indexiter_bkw(ii);
		break;
	case SS_GT:
	case SS_GTE:
		ii->v = ss_rbnext(&ii->index->i, ii->v);
		ii->vcur = NULL;
		sv_indexiter_fwd(ii);
		break;
	default: assert(0);
	}
}

typedef struct svindexiterraw svindexiterraw;

struct svindexiterraw {
	svindex *index;
	ssrbnode *v;
	svv *vcur;
	sv current;
} sspacked;

static inline int
sv_indexiterraw_open(ssiter *i, svindex *index)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	ii->index = index;
	ii->v = ss_rbmin(&ii->index->i);
	ii->vcur = NULL;
	if (ii->v) {
		ii->vcur = sscast(ii->v, svv, node);
	}
	return 0;
}

static inline void
sv_indexiterraw_close(ssiter *i ssunused)
{}

static inline int
sv_indexiterraw_has(ssiter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	return ii->v != NULL;
}

static inline void*
sv_indexiterraw_of(ssiter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	if (ssunlikely(ii->v == NULL))
		return NULL;
	assert(ii->vcur != NULL);
	sv_init(&ii->current, &sv_vif, ii->vcur, NULL);
	return &ii->current;
}

static inline void
sv_indexiterraw_next(ssiter *i)
{
	svindexiterraw *ii = (svindexiterraw*)i->priv;
	if (ssunlikely(ii->v == NULL))
		return;
	assert(ii->vcur != NULL);
	svv *v = ii->vcur->next;
	if (v) {
		ii->vcur = v;
		return;
	}
	ii->v = ss_rbnext(&ii->index->i, ii->v);
	ii->vcur = NULL;
	if (ii->v) {
		ii->vcur = sscast(ii->v, svv, node);
	}
}

extern ssiterif sv_indexiter;
extern ssiterif sv_indexiterraw;

#endif
