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
	char *key;
	sr *r;
} sspacked;

static inline void
sd_pageiter_result(sdpageiter *i)
{
	if (ssunlikely(i->v == NULL))
		return;
	if (sslikely(i->r->fmt_storage == SF_RAW)) {
		sv_init(&i->current, &sd_vif, i->v, i->page->h);
		return;
	}
	sd_pagesparse_convert(i->page, i->r, i->v, i->xfbuf->s);
	sv_init(&i->current, &sd_vrawif, i->xfbuf->s, NULL);
}

static inline void
sd_pageiter_end(sdpageiter *i)
{
	i->pos = i->page->h->count;
	i->v   = NULL;
}

static inline int
sd_pageiter_cmp(sdpageiter *i, sr *r, sdv *v)
{
	if (sslikely(r->fmt_storage == SF_RAW)) {
		return sf_compare(r->scheme, sd_pagepointer(i->page, v), i->key);
	}
	sffield **part = r->scheme->keys;
	sffield **last = part + r->scheme->keys_count;
	int rc;
	while (part < last) {
		sffield *key = *part;
		uint32_t a_fieldsize;
		char *a_field = sd_pagesparse_field(i->page, v, key->position, &a_fieldsize);
		uint32_t b_fieldsize;
		char *b_field = sf_fieldof_ptr(r->scheme, key, i->key, &b_fieldsize);
		rc = key->cmp(a_field, a_fieldsize, b_field, b_fieldsize, NULL);
		if (rc != 0)
			return rc;
		part++;
	}
	return 0;
}

static inline int
sd_pageiter_search(sdpageiter *i)
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
	return min;
}

static inline void
sd_pageiter_chain_head(sdpageiter *i, int64_t pos)
{
	/* find first non-duplicate key */
	while (pos >= 0) {
		sdv *v = sd_pagev(i->page, pos);
		uint8_t flags =
			sf_flags(i->r->scheme, sd_pagepointer(i->page, v));
		if (sslikely(! (flags & SVDUP))) {
			i->pos = pos;
			i->v = v;
			return;
		}
		pos--;
	}
	sd_pageiter_end(i);
}

static inline void
sd_pageiter_chain_next(sdpageiter *i)
{
	/* skip to next duplicate chain */
	int64_t pos = i->pos + 1;
	while (pos < i->page->h->count) {
		sdv *v = sd_pagev(i->page, pos);
		uint8_t flags =
			sf_flags(i->r->scheme, sd_pagepointer(i->page, v));
		if (sslikely(! (flags & SVDUP))) {
			i->pos = pos;
			i->v = v;
			return;
		}
		pos++;
	}
	sd_pageiter_end(i);
}

static inline int
sd_pageiter_gt(sdpageiter *i, int e)
{
	if (i->key == NULL) {
		i->pos = 0;
		i->v = sd_pagev(i->page, i->pos);
		return 0;
	}
	int64_t pos = sd_pageiter_search(i);
	if (ssunlikely(pos >= i->page->h->count))
		pos = i->page->h->count - 1;
	sd_pageiter_chain_head(i, pos);
	if (i->v == NULL)
		return 0;
	int rc = sd_pageiter_cmp(i, i->r, i->v);
	int match = rc == 0;
	switch (rc) {
		case  0:
			if (e) {
				break;
			}
		case -1:
			sd_pageiter_chain_next(i);
			break;
	}
	return match;
}

static inline int
sd_pageiter_lt(sdpageiter *i, int e)
{
	if (i->key == NULL) {
		sd_pageiter_chain_head(i, i->page->h->count - 1);
		return 0;
	}
	int64_t pos = sd_pageiter_search(i);
	if (ssunlikely(pos >= i->page->h->count))
		pos = i->page->h->count - 1;
	sd_pageiter_chain_head(i, pos);
	if (i->v == NULL)
		return 0;
	int rc = sd_pageiter_cmp(i, i->r, i->v);
	int match = rc == 0;
	switch (rc) {
		case 0:
			if (e) {
				break;
			}
		case 1:
			sd_pageiter_chain_head(i, i->pos - 1);
			break;
	}
	return match;
}

static inline int
sd_pageiter_open(ssiter *i, sr *r, ssbuf *xfbuf, sdpage *page, ssorder o,
                 char *key)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	pi->r     = r;
	pi->page  = page;
	pi->xfbuf = xfbuf;
	pi->order = o;
	pi->key   = key;
	pi->v     = NULL;
	pi->pos   = 0;
	if (ssunlikely(pi->page->h->count == 0)) {
		sd_pageiter_end(pi);
		return 0;
	}
	int rc = 0;
	switch (pi->order) {
	case SS_GT:  rc = sd_pageiter_gt(pi, 0);
		break;
	case SS_GTE: rc = sd_pageiter_gt(pi, 1);
		break;
	case SS_LT:  rc = sd_pageiter_lt(pi, 0);
		break;
	case SS_LTE: rc = sd_pageiter_lt(pi, 1);
		break;
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
	if (pi->v == NULL)
		return;
	switch (pi->order) {
	case SS_GTE:
	case SS_GT:
		pi->pos++;
		if (ssunlikely(pi->pos >= pi->page->h->count)) {
			sd_pageiter_end(pi);
			return;
		}
		pi->v = sd_pagev(pi->page, pi->pos);
		break;
	case SS_LT:
	case SS_LTE: {
		/* key (dup) (dup) key (eof) */
		sdv *v;
		int64_t pos = pi->pos + 1;
		if (pos < pi->page->h->count) {
			v = sd_pagev(pi->page, pos);
			uint8_t flags =
				sf_flags(pi->r->scheme, sd_pagepointer(pi->page, v));
			if (flags & SVDUP) {
				pi->pos = pos;
				pi->v   = v;
				break;
			}
		}
		/* skip current chain and position to
		 * the previous one */
		sd_pageiter_chain_head(pi, pi->pos);
		sd_pageiter_chain_head(pi, pi->pos - 1);
		break;
	}
	default: assert(0);
	}
	sd_pageiter_result(pi);
}

extern ssiterif sd_pageiter;

#endif
