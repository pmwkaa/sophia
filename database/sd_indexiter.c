
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

typedef struct sdindexiter sdindexiter;

struct sdindexiter {
	sdindex *index;
	sdindexpage *v;
	int pos;
	srorder cmp;
	void *key;
	int keysize;
} srpacked;

static void
sd_indexiter_init(sriter *i)
{
	assert(sizeof(sdindexiter) <= sizeof(i->priv));
	sdindexiter *ii = (sdindexiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline int
sd_indexiter_seek(sriter *i, void *key, int size, int *minp, int *midp, int *maxp)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	int match = 0;
	int min = 0;
	int max = ii->index->h->count - 1;
	int mid = 0;
	while (max >= min)
	{
		mid = min + ((max - min) >> 1);
		sdindexpage *page = sd_indexpage(ii->index, mid);

		int rc = sd_indexpage_cmp(page, key, size, i->r->cmp);
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

static int
sd_indexiter_route(sriter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	int mid, min, max;
	int rc = sd_indexiter_seek(i, ii->key, ii->keysize, &min, &mid, &max);
	if (srlikely(rc))
		return mid;
	if (srunlikely(min >= (int)ii->index->h->count))
		min = ii->index->h->count - 1;
	return min;
}

static int
sd_indexiter_open(sriter *i, va_list args)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	ii->index   = va_arg(args, sdindex*);
	ii->cmp     = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->v       = NULL;
	if (ii->index->h->count == 1) {
		ii->pos = 0;
		if (ii->index->h->lsnmin == 0 && 
		    ii->index->h->lsnmax == 0) {
			/* skip bootstrap node  */
			return 0;
		} 
		ii->v = sd_indexpage(ii->index, ii->pos);
		return 0;
	}
	if (ii->key == NULL) {
		switch (ii->cmp) {
		case SR_LT:
		case SR_LTE: ii->pos = ii->index->h->count - 1;
			break;
		case SR_GT:
		case SR_GTE: ii->pos = 0;
			break;
		default:
			assert(0);
		}
	} else {
		ii->pos = sd_indexiter_route(i);
		sdindexpage *p = sd_indexpage(ii->index, ii->pos);
		switch (ii->cmp) {
		case SR_LTE: break;
		case SR_LT: {
			int l = sr_compare(i->r->cmp, sd_indexpage_min(p), p->sizemin,
			                   ii->key, ii->keysize);
			if (srunlikely(l == 0))
				ii->pos--;
			break;
		}
		case SR_GTE: break;
		case SR_GT: {
			int r = sr_compare(i->r->cmp, sd_indexpage_max(p), p->sizemax,
			                   ii->key, ii->keysize);
			if (srunlikely(r == 0))
				ii->pos++;
			break;
		}
		default: assert(0);
		}
	}
	if (srunlikely(ii->pos == -1 ||
	               ii->pos >= (int)ii->index->h->count))
		return 0;
	ii->v = sd_indexpage(ii->index, ii->pos);
	return 0;
}

static void
sd_indexiter_close(sriter *i srunused)
{ }

static int
sd_indexiter_has(sriter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	return ii->v != NULL;
}

static void*
sd_indexiter_of(sriter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	return ii->v;
}

static void
sd_indexiter_next(sriter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	switch (ii->cmp) {
	case SR_LT:
	case SR_LTE:
		ii->pos--;
		break;
	case SR_GT:
	case SR_GTE:
		ii->pos++;
		break;
	default:
		assert(0);
		break;
	}
	if (srunlikely(ii->pos < 0))
		ii->v = NULL;
	else
	if (srunlikely(ii->pos >= (int)ii->index->h->count))
		ii->v = NULL;
	else
		ii->v = sd_indexpage(ii->index, ii->pos);
}

sriterif sd_indexiter =
{
	.init  = sd_indexiter_init,
	.open  = sd_indexiter_open,
	.close = sd_indexiter_close,
	.has   = sd_indexiter_has,
	.of    = sd_indexiter_of,
	.next  = sd_indexiter_next
};
