#ifndef SV_MERGEITER_H_
#define SV_MERGEITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

/*
 * Merge serveral sorted streams into one.
 * Track duplicates.
 *
 * Merger does not recognize duplicates from
 * a single stream, assumed that they are tracked
 * by the incoming data sources.
*/

typedef struct svmergeiter svmergeiter;

struct svmergeiter {
	srorder order;
	svmerge *merge;
	svmergesrc *src, *end;
	svmergesrc *v;
} srpacked;

static inline void
sv_mergeiter_dupreset(svmergeiter *im, svmergesrc *pos)
{
	svmergesrc *v = im->src;
	while (v != pos) {
		v->dup = 0;
		v = sv_mergenextof(v);
	}
}

static inline void
sv_mergeiter_gt(sriter *it)
{
	svmergeiter *im = (svmergeiter*)it->priv;
	if (im->v) {
		im->v->dup = 0;
		sr_iteratornext(im->v->i);
	}
	im->v = NULL;
	svmergesrc *min, *src;
	sv *minv;
	minv = NULL;
	min  = NULL;
	src  = im->src;
	for (; src < im->end; src = sv_mergenextof(src))
	{
		sv *v = sr_iteratorof(src->i);
		if (v == NULL)
			continue;
		if (min == NULL) {
			minv = v;
			min = src;
			continue;
		}
		int rc = sv_compare(minv, v, it->r->cmp);
		switch (rc) {
		case 0:
			assert(sv_lsn(v) < sv_lsn(minv));
			src->dup = 1;
			break;
		case 1:
			sv_mergeiter_dupreset(im, src);
			minv = v;
			min = src;
			break;
		}
	}
	if (srunlikely(min == NULL))
		return;
	im->v = min;
}

static inline void
sv_mergeiter_lt(sriter *it)
{
	svmergeiter *im = (svmergeiter*)it->priv;
	if (im->v) {
		im->v->dup = 0;
		sr_iteratornext(im->v->i);
	}
	im->v = NULL;
	svmergesrc *max, *src;
	sv *maxv;
	maxv = NULL;
	max  = NULL;
	src  = im->src;
	for (; src < im->end; src = sv_mergenextof(src))
	{
		sv *v = sr_iteratorof(src->i);
		if (v == NULL)
			continue;
		if (max == NULL) {
			maxv = v;
			max = src;
			continue;
		}
		int rc = sv_compare(maxv, v, it->r->cmp);
		switch (rc) {
		case  0:
			assert(sv_lsn(v) < sv_lsn(maxv));
			src->dup = 1;
			break;
		case -1:
			sv_mergeiter_dupreset(im, src);
			maxv = v;
			max = src;
			break;
		}
	}
	if (srunlikely(max == NULL))
		return;
	im->v = max;
}

static inline void
sv_mergeiter_next(sriter *it)
{
	svmergeiter *im = (svmergeiter*)it->priv;
	switch (im->order) {
	case SR_GT:
	case SR_GTE:
		sv_mergeiter_gt(it);
		break;
	case SR_LT:
	case SR_LTE:
		sv_mergeiter_lt(it);
		break;
	default: assert(0);
	}
}

static inline int
sv_mergeiter_open(sriter *i, svmerge *m, srorder o)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	im->merge = m;
	im->order = o;
	im->src   = (svmergesrc*)(im->merge->buf.s);
	im->end   = (svmergesrc*)(im->merge->buf.p);
	im->v     = NULL;
	sv_mergeiter_next(i);
	return 0;
}

static inline void
sv_mergeiter_close(sriter *i srunused)
{ }

static inline int
sv_mergeiter_has(sriter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	return im->v != NULL;
}

static inline void*
sv_mergeiter_of(sriter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return sr_iteratorof(im->v->i);
}

static inline uint32_t
sv_mergeisdup(sriter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	assert(im->v != NULL);
	if (im->v->dup)
		return SVDUP;
	return 0;
}

extern sriterif sv_mergeiter;

#endif
