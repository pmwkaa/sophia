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
	srbuf *xfbuf;
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
sd_pageiter_cmp(sdpageiter *pi, sr *r, sdv *v)
{
	uint64_t size, lsn;
	if (srlikely(r->fmt_storage == SR_FS_RAW)) {
		char *key = sd_pagemetaof(pi->page, v, &size, &lsn);
		return sr_compare(r->scheme, key, size, pi->key, pi->keysize);
	}
	/* key-value */
	srkey *part = r->scheme->parts;
	srkey *last = part + r->scheme->count;
	int rc;
	while (part < last) {
		char *key = sd_pagekv_key(pi->page, v, &size, part->pos);
		rc = part->cmpraw(key, size,
		                  sr_fmtkey(pi->key, part->pos),
		                  sr_fmtkey_size(pi->key, part->pos),
		                  NULL);
		if (rc != 0)
			return rc;
		part++;
	}
	return 0;
}

static inline int
sd_pageiter_search(sriter *i, int search_min)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	int min = 0;
	int mid = 0;
	int max = pi->page->h->count - 1;
	while (max >= min)
	{
		mid = min + (max - min) / 2;
		int rc = sd_pageiter_cmp(pi, i->r, sd_pagev(pi->page, mid));
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
		uint64_t lsn = sd_pagelsnof(i->page, v);
		if (lsn <= i->vlsn) {
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
		uint64_t lsn = sd_pagelsnof(i->page, v);
		if (lsn <= i->vlsn) {
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
	uint64_t lsn = sd_pagelsnof(i->page, i->v);
	if (lsn == i->vlsn)
		return;
	if (lsn > i->vlsn) {
		/* search max < i->vlsn */
		pos++;
		while (pos < i->page->h->count)
		{
			sdv *v = sd_pagev(i->page, pos);
			lsn = sd_pagelsnof(i->page, v);
			if (! (v->flags & SVDUP))
				break;
			if (lsn <= i->vlsn) {
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
	uint64_t lsn = sd_pagelsnof(i->page, i->v);
	if (lsn == i->vlsn)
		return;

	if (lsn > i->vlsn) {
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
		lsn = sd_pagelsnof(i->page, v);
		if (lsn <= i->vlsn) {
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
		uint64_t lsn = sd_pagelsnof(i->page, v);
		if (lsn <= i->vlsn) {
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
	int rc = sd_pageiter_cmp(pi, i->r, pi->v);
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
	int rc = sd_pageiter_cmp(pi, i->r, pi->v);
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

static inline void
sd_pageiter_result(sdpageiter *i, sr *r)
{
	if (srunlikely(i->v == NULL))
		return;
	if (srlikely(r->fmt_storage == SR_FS_RAW)) {
		sv_init(&i->current, &sd_vif, i->v, i->page->h);
		return;
	}
	sd_pagekv_convert(i->page, r, i->v, i->xfbuf->s);
	sv_init(&i->current, &sd_vrawif, i->xfbuf->s, NULL);
}

static inline int
sd_pageiter_open(sriter *i, srbuf *xfbuf, sdpage *page, srorder o,
                 void *key, int keysize, uint64_t vlsn)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	pi->page    = page;
	pi->xfbuf   = xfbuf;
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
	int rc;
	switch (pi->order) {
	case SR_LT:  rc = sd_pageiter_lt(i, 0);
		break;
	case SR_LTE: rc = sd_pageiter_lt(i, 1);
		break;
	case SR_GT:  rc = sd_pageiter_gt(i, 0);
		break;
	case SR_GTE: rc = sd_pageiter_gt(i, 1);
		break;
	case SR_EQ:  rc = sd_pageiter_lt(i, 1);
		break;
	case SR_UPDATE: {
		uint64_t vlsn = pi->vlsn;
		pi->vlsn = (uint64_t)-1;
		match = sd_pageiter_lt(i, 1);
		if (match == 0)
			return 0;
		rc = sd_pagelsnof(pi->page, pi->v) > vlsn;
		break;
	}
	default: assert(0);
	}
	sd_pageiter_result(pi, i->r);
	return rc;
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
	sd_pageiter_result(pi, i->r);
}

extern sriterif sd_pageiter;

#endif
