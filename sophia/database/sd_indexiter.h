#ifndef SD_INDEXITER_H_
#define SD_INDEXITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdindexiter sdindexiter;

struct sdindexiter {
	sdindex *index;
	sdindexpage *v;
	int pos;
	ssorder cmp;
	void *key;
	int keysize;
	sr *r;
} sspacked;

static inline int
sd_indexiter_seek(sdindexiter *i, void *key, int size, int *minp, int *midp, int *maxp)
{
	int match = 0;
	int min = 0;
	int max = i->index->h->count - 1;
	int mid = 0;
	while (max >= min)
	{
		mid = min + ((max - min) >> 1);
		sdindexpage *page = sd_indexpage(i->index, mid);

		int rc = sd_indexpage_cmp(i->index, page, key, size, i->r->scheme);
		switch (rc) {
		case -1: min = mid + 1;
			continue;
		case  1: max = mid - 1;
			continue;
		default: match = 1;
			goto done;
		}
	}
done:
	*minp = min;
	*midp = mid;
	*maxp = max;
	return match;
}

static inline int
sd_indexiter_route(sdindexiter *i)
{
	int mid, min, max;
	int rc = sd_indexiter_seek(i, i->key, i->keysize, &min, &mid, &max);
	if (sslikely(rc))
		return mid;
	if (ssunlikely(min >= (int)i->index->h->count))
		min = i->index->h->count - 1;
	return min;
}

static inline int
sd_indexiter_open(ssiter *i, sr *r, sdindex *index, ssorder o, void *key, int keysize)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	ii->r       = r;
	ii->index   = index;
	ii->cmp     = o;
	ii->key     = key;
	ii->keysize = keysize;
	ii->v       = NULL;
	ii->pos     = 0;
	if (ii->index->h->count == 1) {
		ii->pos = 0;
		if (ii->index->h->lsnmin == UINT64_MAX &&
		    ii->index->h->lsnmax == 0) {
			/* skip bootstrap node  */
			return 0;
		}
		ii->v = sd_indexpage(ii->index, ii->pos);
		return 0;
	}
	if (ii->key == NULL) {
		switch (ii->cmp) {
		case SS_LT:
		case SS_LTE: ii->pos = ii->index->h->count - 1;
			break;
		case SS_GT:
		case SS_GTE: ii->pos = 0;
			break;
		default:
			assert(0);
		}
	} else {
		ii->pos = sd_indexiter_route(ii);
		sdindexpage *p = sd_indexpage(ii->index, ii->pos);
		switch (ii->cmp) {
		case SS_LTE: break;
		case SS_LT: {
			int l = sr_compare(ii->r->scheme, sd_indexpage_min(ii->index, p), p->sizemin,
			                   ii->key, ii->keysize);
			if (ssunlikely(l == 0))
				ii->pos--;
			break;
		}
		case SS_GTE: break;
		case SS_GT: {
			int r = sr_compare(ii->r->scheme, sd_indexpage_max(ii->index, p), p->sizemax,
			                   ii->key, ii->keysize);
			if (ssunlikely(r == 0))
				ii->pos++;
			break;
		}
		default: assert(0);
		}
	}
	if (ssunlikely(ii->pos == -1 ||
	               ii->pos >= (int)ii->index->h->count))
		return 0;
	ii->v = sd_indexpage(ii->index, ii->pos);
	return 0;
}

static inline void
sd_indexiter_close(ssiter *i ssunused)
{ }

static inline int
sd_indexiter_has(ssiter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	return ii->v != NULL;
}

static inline void*
sd_indexiter_of(ssiter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	return ii->v;
}

static inline void
sd_indexiter_next(ssiter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	switch (ii->cmp) {
	case SS_LT:
	case SS_LTE:
		ii->pos--;
		break;
	case SS_GT:
	case SS_GTE:
		ii->pos++;
		break;
	default:
		assert(0);
		break;
	}
	if (ssunlikely(ii->pos < 0))
		ii->v = NULL;
	else
	if (ssunlikely(ii->pos >= (int)ii->index->h->count))
		ii->v = NULL;
	else
		ii->v = sd_indexpage(ii->index, ii->pos);
}

extern ssiterif sd_indexiter;

#endif
