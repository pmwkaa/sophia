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
	ssbuf *xfbuf;
	int64_t pos;
	sdv *v;
	sv current;
	ssorder order;
	void *key;
	int keysize;
	uint64_t vlsn;
	sr *r;
} sspacked;

static inline void
sd_pageiter_end(sdpageiter *i)
{
	i->pos = i->page->h->count;
	i->v   = NULL;
}

static inline int
sd_pageiter_cmp(sdpageiter *i, sr *r, sdv *v)
{
	uint64_t size, lsn;
	if (sslikely(r->fmt_storage == SF_SRAW)) {
		char *key = sd_pagemetaof(i->page, v, &size, &lsn);
		return sr_compare(r->scheme, key, size, i->key, i->keysize);
	}
	/* key-value */
	srkey *part = r->scheme->parts;
	srkey *last = part + r->scheme->count;
	int rc;
	while (part < last) {
		char *key = sd_pagekv_key(i->page, v, &size, part->pos);
		rc = part->cmpraw(key, size,
		                  sf_key(i->key, part->pos),
		                  sf_keysize(i->key, part->pos),
		                  NULL);
		if (rc != 0)
			return rc;
		part++;
	}
	return 0;
}

static inline int
sd_pageiter_search(sdpageiter *i, int search_min)
{
	int min = 0;
	int mid = 0;
	int max = i->page->h->count - 1;
	while (max >= min)
	{
		mid = min + (max - min) / 2;
		int rc = sd_pageiter_cmp(i, i->r, sd_pagev(i->page, mid));
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
		if (ssunlikely(pos < 0)) {
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
	if (ssunlikely(pos >= i->page->h->count)) {
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
	if (ssunlikely(pos == i->page->h->count)) {
		sd_pageiter_end(i);
		return;
	}
	assert(match != NULL);
	i->pos = pos;
	i->v = match;
}

static inline int
sd_pageiter_lt(sdpageiter *i, int e)
{
	if (ssunlikely(i->page->h->count == 0)) {
		sd_pageiter_end(i);
		return 0;
	}
	if (i->key == NULL) {
		sd_pageiter_lv(i, i->page->h->count - 1);
		return 0;
	}
	int64_t pos = sd_pageiter_search(i, 1);
	if (ssunlikely(pos >= i->page->h->count))
		pos = i->page->h->count - 1;
	sd_pageiter_lland(i, pos);
	if (i->v == NULL)
		return 0;
	int rc = sd_pageiter_cmp(i, i->r, i->v);
	int match = rc == 0;
	switch (rc) {
		case  0:
			if (! e)
				sd_pageiter_bkw(i);
			break;
		case  1:
			sd_pageiter_bkw(i);
			break;
	}
	return match;
}

static inline int
sd_pageiter_gt(sdpageiter *i, int e)
{
	if (ssunlikely(i->page->h->count == 0)) {
		sd_pageiter_end(i);
		return 0;
	}
	if (i->key == NULL) {
		sd_pageiter_gv(i, 0);
		return 0;
	}
	int64_t pos = sd_pageiter_search(i, 1);
	if (ssunlikely(pos >= i->page->h->count))
		pos = i->page->h->count - 1;
	sd_pageiter_gland(i, pos);
	if (i->v == NULL)
		return 0;
	int rc = sd_pageiter_cmp(i, i->r, i->v);
	int match = rc == 0;
	switch (rc) {
		case  0:
			if (! e)
				sd_pageiter_fwd(i);
			break;
		case -1:
			sd_pageiter_fwd(i);
			break;
	}
	return match;
}

static inline void
sd_pageiter_result(sdpageiter *i)
{
	if (ssunlikely(i->v == NULL))
		return;
	if (sslikely(i->r->fmt_storage == SF_SRAW)) {
		sv_init(&i->current, &sd_vif, i->v, i->page->h);
		return;
	}
	sd_pagekv_convert(i->page, i->r, i->v, i->xfbuf->s);
	sv_init(&i->current, &sd_vrawif, i->xfbuf->s, NULL);
}

static inline int
sd_pageiter_open(ssiter *i, sr *r, ssbuf *xfbuf, sdpage *page, ssorder o,
                 void *key, int keysize, uint64_t vlsn)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	pi->r       = r;
	pi->page    = page;
	pi->xfbuf   = xfbuf;
	pi->order   = o;
	pi->key     = key;
	pi->keysize = keysize;
	pi->vlsn    = vlsn;
	pi->v       = NULL;
	pi->pos     = 0;
	if (ssunlikely(pi->page->h->lsnmin > pi->vlsn &&
	               pi->order != SS_UPDATE))
		return 0;
	int rc = 0;
	switch (pi->order) {
	case SS_LT:  rc = sd_pageiter_lt(pi, 0);
		break;
	case SS_LTE: rc = sd_pageiter_lt(pi, 1);
		break;
	case SS_GT:  rc = sd_pageiter_gt(pi, 0);
		break;
	case SS_GTE: rc = sd_pageiter_gt(pi, 1);
		break;
	case SS_EQ:  rc = sd_pageiter_lt(pi, 1);
		break;
	case SS_UPDATE: {
		uint64_t vlsn = pi->vlsn;
		pi->vlsn = (uint64_t)-1;
		int match = sd_pageiter_lt(pi, 1);
		if (match == 0)
			return 0;
		rc = sd_pagelsnof(pi->page, pi->v) > vlsn;
		break;
	}
	default: assert(0);
	}
	sd_pageiter_result(pi);
	return rc;
}

static inline void
sd_pageiter_close(ssiter *i ssunused)
{ }

static inline int
sd_pageiter_has(ssiter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	return pi->v != NULL;
}

static inline void*
sd_pageiter_of(ssiter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	if (ssunlikely(pi->v == NULL))
		return NULL;
	return &pi->current;
}

static inline void
sd_pageiter_next(ssiter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	switch (pi->order) {
	case SS_LT:
	case SS_LTE: sd_pageiter_bkw(pi);
		break;
	case SS_GT:
	case SS_GTE: sd_pageiter_fwd(pi);
		break;
	default: assert(0);
	}
	sd_pageiter_result(pi);
}

extern ssiterif sd_pageiter;

#endif
