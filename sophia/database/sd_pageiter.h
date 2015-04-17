#ifndef SD_PAGEITER_H_
#define SD_PAGEITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdpageiter sdpageiter;

struct sdpageiter {
	sdpage *page;
	int64_t pos;
	sdv *v;
	sv current;
	srorder order;
	void *key;
	int keysize;
	uint64_t vlsn;
} srpacked;

static inline void
sd_pageiter_end(sdpageiter *i)
{
	i->pos = i->page->h->count;
	i->v   = NULL;
}

static inline int
sd_pageiter_search(sriter *i, int search_min)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	srkey *cmp = i->r->cmp;
	int min = 0;
	int mid = 0;
	int max = pi->page->h->count - 1;
	while (max >= min)
	{
		mid = min + (max - min) / 2;
		sdv *v = sd_pagev(pi->page, mid);
		char *key = sd_pageof(pi->page, v);
		int rc = sr_compare(cmp, key, v->size, pi->key, pi->keysize);
		switch (rc) {
		case -1: min = mid + 1;
			continue;
		case  1: max = mid - 1;
			continue;
		default: return mid;
		}
	}
	return (search_min) ? min : max;
}

static inline void
sd_pageiter_lv(sdpageiter *i, int64_t pos)
{
	/* lower-visible bound */

	/* find visible max: any first key which
	 * lsn <= vlsn (max in dup chain) */
	int64_t maxpos = 0;
	sdv *v;
	sdv *max = NULL;
	while (pos >= 0) {
		v = sd_pagev(i->page, pos);
		if (v->lsn <= i->vlsn) {
			maxpos = pos;
			max = v;
		}
		if (! (v->flags & SVDUP)) {
			/* head */
			if (max) {
				i->pos = maxpos;
				i->v = max;
				return;
			}
		}
		pos--;
	}
	sd_pageiter_end(i);
}

static inline void
sd_pageiter_gv(sdpageiter *i, int64_t pos)
{
	/* greater-visible bound */

	/* find visible max: any first key which
	 * lsn <= vlsn (max in dup chain) */
	while (pos < i->page->h->count ) {
		sdv *v = sd_pagev(i->page, pos);
		if (v->lsn <= i->vlsn) {
			i->pos = pos;
			i->v = v;
			return;
		}
		pos++;
	}
	sd_pageiter_end(i);
}

static inline void
sd_pageiter_lland(sdpageiter *i, int64_t pos)
{
	/* reposition to a visible duplicate */
	i->pos = pos;
	i->v = sd_pagev(i->page, i->pos);
	if (i->v->lsn == i->vlsn)
		return;
	if (i->v->lsn > i->vlsn) {
		/* search max < i->vlsn */
		pos++;
		while (pos < i->page->h->count)
		{
			sdv *v = sd_pagev(i->page, pos);
			if (! (v->flags & SVDUP))
				break;
			if (v->lsn <= i->vlsn) {
				i->pos = pos;
				i->v = v;
				return;
			}
			pos++;
		}
	}
	sd_pageiter_lv(i, i->pos);
}

static inline void
sd_pageiter_gland(sdpageiter *i, int64_t pos)
{
	/* reposition to a visible duplicate */
	i->pos = pos;
	i->v = sd_pagev(i->page, i->pos);
	if (i->v->lsn == i->vlsn)
		return;

	if (i->v->lsn > i->vlsn) {
		/* search max < i->vlsn */
		pos++;
		sd_pageiter_gv(i, pos);
		return;
	}

	/* i->v->lsn < i->vlsn */
	if (! (i->v->flags & SVDUP))
		return;
	int64_t maxpos = pos;
	sdv *max = sd_pagev(i->page, i->pos);
	pos--;
	while (pos >= 0) {
		sdv *v = sd_pagev(i->page, pos);
		if (v->lsn <= i->vlsn) {
			maxpos = pos;
			max = v;
		}
		if (! (v->flags & SVDUP))
			break;
		pos--;
	}
	i->pos = maxpos;
	i->v = max;
}

static inline void
sd_pageiter_bkw(sdpageiter *i)
{
	/* skip to previous visible key */
	int64_t pos = i->pos;
	sdv *v = sd_pagev(i->page, pos);
	if (v->flags & SVDUP) {
		/* skip duplicates */
		pos--;
		while (pos >= 0) {
			v = sd_pagev(i->page, pos);
			if (! (v->flags & SVDUP))
				break;
			pos--;
		}
		if (srunlikely(pos < 0)) {
			sd_pageiter_end(i);
			return;
		}
	}
	assert(! (v->flags & SVDUP));
	pos--;

	sd_pageiter_lv(i, pos);
}

static inline void
sd_pageiter_fwd(sdpageiter *i)
{
	/* skip to next visible key */
	int64_t pos = i->pos + 1;
	while (pos < i->page->h->count)
	{
		sdv *v = sd_pagev(i->page, pos);
		if (! (v->flags & SVDUP))
			break;
		pos++;
	}
	if (srunlikely(pos >= i->page->h->count)) {
		sd_pageiter_end(i);
		return;
	}
	sdv *match = NULL;
	while (pos < i->page->h->count)
	{
		sdv *v = sd_pagev(i->page, pos);
		if (v->lsn <= i->vlsn) {
			match = v;
			break;
		}
		pos++;
	}
	if (srunlikely(pos == i->page->h->count)) {
		sd_pageiter_end(i);
		return;
	}
	assert(match != NULL);
	i->pos = pos;
	i->v = match;
}

static inline int
sd_pageiter_lt(sriter *i, int e)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	if (srunlikely(pi->page->h->count == 0)) {
		sd_pageiter_end(pi);
		return 0;
	}
	if (pi->key == NULL) {
		sd_pageiter_lv(pi, pi->page->h->count - 1);
		return 0;
	}
	int64_t pos = sd_pageiter_search(i, 1);
	if (srunlikely(pos >= pi->page->h->count))
		pos = pi->page->h->count - 1;
	sd_pageiter_lland(pi, pos);
	if (pi->v == NULL)
		return 0;
	char *key = sd_pageof(pi->page, pi->v);
	int rc = sr_compare(i->r->cmp, key, pi->v->size,
	                    pi->key,
	                    pi->keysize);
	int match = rc == 0;
	switch (rc) {
		case  0:
			if (! e)
				sd_pageiter_bkw(pi);
			break;
		case  1:
			sd_pageiter_bkw(pi);
			break;
	}
	return match;
}

static inline int
sd_pageiter_gt(sriter *i, int e)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	if (srunlikely(pi->page->h->count == 0)) {
		sd_pageiter_end(pi);
		return 0;
	}
	if (pi->key == NULL) {
		sd_pageiter_gv(pi, 0);
		return 0;
	}
	int64_t pos = sd_pageiter_search(i, 1);
	if (srunlikely(pos >= pi->page->h->count))
		pos = pi->page->h->count - 1;
	sd_pageiter_gland(pi, pos);
	if (pi->v == NULL)
		return 0;
	char *key = sd_pageof(pi->page, pi->v);
	int rc = sr_compare(i->r->cmp, key, pi->v->size,
	                    pi->key,
	                    pi->keysize);
	int match = rc == 0;
	switch (rc) {
		case  0:
			if (! e)
				sd_pageiter_fwd(pi);
			break;
		case -1:
			sd_pageiter_fwd(pi);
			break;
	}
	return match;
}

static inline int
sd_pageiter_open(sriter *i, sdpage *page, srorder o, void *key, int keysize, uint64_t vlsn)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	pi->page    = page;
	pi->order   = o;
	pi->key     = key;
	pi->keysize = keysize;
	pi->vlsn    = vlsn;
	pi->v       = NULL;
	pi->pos     = 0;
	if (srunlikely(pi->page->h->lsnmin > pi->vlsn &&
	               pi->order != SR_UPDATE))
		return 0;
	int match;
	switch (pi->order) {
	case SR_LT:     return sd_pageiter_lt(i, 0);
	case SR_LTE:    return sd_pageiter_lt(i, 1);
	case SR_GT:     return sd_pageiter_gt(i, 0);
	case SR_GTE:    return sd_pageiter_gt(i, 1);
	case SR_EQ:     return sd_pageiter_lt(i, 1);
	case SR_UPDATE: {
		uint64_t vlsn = pi->vlsn;
		pi->vlsn = (uint64_t)-1;
		match = sd_pageiter_lt(i, 1);
		if (match == 0)
			return 0;
		return pi->v->lsn > vlsn;
	}
	default: assert(0);
	}
	return 0;
}

static inline void
sd_pageiter_close(sriter *i srunused)
{ }

static inline int
sd_pageiter_has(sriter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	return pi->v != NULL;
}

static inline void*
sd_pageiter_of(sriter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	if (srunlikely(pi->v == NULL))
		return NULL;
	sv_init(&pi->current, &sd_vif, pi->v, pi->page->h);
	return &pi->current;
}

static inline void
sd_pageiter_next(sriter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	switch (pi->order) {
	case SR_LT:
	case SR_LTE: sd_pageiter_bkw(pi);
		break;
	case SR_GT:
	case SR_GTE: sd_pageiter_fwd(pi);
		break;
	default: assert(0);
	}
}

typedef struct sdpageiterraw sdpageiterraw;

struct sdpageiterraw {
	sdpage *page;
	int64_t pos;
	sdv *v;
	sv current;
} srpacked;

static inline int
sd_pageiterraw_open(sriter *i, sdpage *page)
{
	sdpageiterraw *pi = (sdpageiterraw*)i->priv;
	sdpage *p = page;
	pi->page = p;
	if (srunlikely(p->h->count == 0)) {
		pi->pos = 1;
		pi->v = NULL;
		return 0;
	}
	pi->pos = 0;
	pi->v = sd_pagev(p, 0);
	return 0;
}

static inline void
sd_pageiterraw_close(sriter *i srunused)
{ }

static inline int
sd_pageiterraw_has(sriter *i)
{
	sdpageiterraw *pi = (sdpageiterraw*)i->priv;
	return pi->v != NULL;
}

static inline void*
sd_pageiterraw_of(sriter *i)
{
	sdpageiterraw *pi = (sdpageiterraw*)i->priv;
	if (srunlikely(pi->v == NULL))
		return NULL;
	sv_init(&pi->current, &sd_vif, pi->v, pi->page->h);
	return &pi->current;
}

static inline void
sd_pageiterraw_next(sriter *i)
{
	sdpageiterraw *pi = (sdpageiterraw*)i->priv;
	pi->pos++;
	if (srlikely(pi->pos < pi->page->h->count))
		pi->v = sd_pagev(pi->page, pi->pos);
	else
		pi->v = NULL;
}

extern sriterif sd_pageiter;
extern sriterif sd_pageiterraw;

#endif
