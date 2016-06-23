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
	sdpage  *page;
	int64_t  pos;
	char    *current;
	ssorder  order;
	char    *key;
	sr      *r;
} sspacked;

static inline void
sd_pageiter_result(sdpageiter *i)
{
	if (ssunlikely(i->pos == i->page->h->count)) {
		i->current = NULL;
		return;
	}
	i->current = sd_pagepointer(i->page, i->r, i->pos);
}

static inline void
sd_pageiter_end(sdpageiter *i)
{
	i->pos = i->page->h->count;
	i->current = NULL;
}

static inline int
sd_pageiter_cmp(sdpageiter *i, sr *r, uint32_t pos)
{
	return sf_compare(r->scheme, sd_pagepointer(i->page, i->r, pos), i->key);
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
		int rc = sd_pageiter_cmp(i, i->r, mid);
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
		uint8_t flags =
			sf_flags(i->r->scheme,
			         sd_pagepointer(i->page, i->r, pos));
		if (sslikely(! (flags & SVDUP))) {
			i->pos = pos;
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
		uint8_t flags =
			sf_flags(i->r->scheme,
			         sd_pagepointer(i->page, i->r, pos));
		if (sslikely(! (flags & SVDUP))) {
			i->pos = pos;
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
		return 0;
	}
	int64_t pos = sd_pageiter_search(i);
	if (ssunlikely(pos >= i->page->h->count))
		pos = i->page->h->count - 1;
	sd_pageiter_chain_head(i, pos);
	if (i->pos == i->page->h->count)
		return 0;
	int rc = sd_pageiter_cmp(i, i->r, i->pos);
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
	if (i->pos == i->page->h->count)
		return 0;
	int rc = sd_pageiter_cmp(i, i->r, i->pos);
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
sd_pageiter_open(ssiter *i, sr *r, sdpage *page, ssorder o,
                 char *key)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	pi->r     = r;
	pi->page  = page;
	pi->order = o;
	pi->key   = key;
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
	return pi->current != NULL;
}

static inline void*
sd_pageiter_of(ssiter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	return pi->current;
}

static inline void
sd_pageiter_next(ssiter *i)
{
	sdpageiter *pi = (sdpageiter*)i->priv;
	if (pi->current == NULL)
		return;
	switch (pi->order) {
	case SS_GTE:
	case SS_GT:
		pi->pos++;
		if (ssunlikely(pi->pos >= pi->page->h->count)) {
			sd_pageiter_end(pi);
			return;
		}
		break;
	case SS_LT:
	case SS_LTE: {
		/* key (dup) (dup) key (eof) */
		int64_t pos = pi->pos + 1;
		if (pos < pi->page->h->count) {
			uint8_t flags =
				sf_flags(pi->r->scheme,
				         sd_pagepointer(pi->page, pi->r, pos));
			if (flags & SVDUP) {
				pi->pos = pos;
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
