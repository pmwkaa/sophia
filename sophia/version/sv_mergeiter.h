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
	ssorder order;
	svmerge *merge;
	svmergesrc *src, *end;
	svmergesrc *v;
	sr *r;
} sspacked;

static inline void
sv_mergeiter_dupreset(svmergeiter *i, svmergesrc *pos)
{
	svmergesrc *v = i->src;
	while (v != pos) {
		v->dup = 0;
		v = sv_mergenextof(v);
	}
}

static inline void
sv_mergeiter_gt(svmergeiter *i)
{
	if (i->v) {
		i->v->dup = 0;
		ss_iteratornext(i->v->i);
	}
	i->v = NULL;
	svmergesrc *min, *src;
	sv *minv;
	minv = NULL;
	min  = NULL;
	src  = i->src;
	for (; src < i->end; src = sv_mergenextof(src))
	{
		sv *v = ss_iteratorof(src->i);
		if (v == NULL)
			continue;
		if (min == NULL) {
			minv = v;
			min = src;
			continue;
		}
		int rc;
		rc = sf_compare(i->r->scheme,
		                sv_pointer(minv), sv_size(minv),
		                sv_pointer(v), sv_size(v));
		switch (rc) {
		case 0:
			/*
			assert(sv_lsn(v) < sv_lsn(minv));
			*/
			src->dup = 1;
			break;
		case 1:
			sv_mergeiter_dupreset(i, src);
			minv = v;
			min = src;
			break;
		}
	}
	if (ssunlikely(min == NULL))
		return;
	i->v = min;
}

static inline void
sv_mergeiter_lt(svmergeiter *i)
{
	if (i->v) {
		i->v->dup = 0;
		ss_iteratornext(i->v->i);
	}
	i->v = NULL;
	svmergesrc *max, *src;
	sv *maxv;
	maxv = NULL;
	max  = NULL;
	src  = i->src;
	for (; src < i->end; src = sv_mergenextof(src))
	{
		sv *v = ss_iteratorof(src->i);
		if (v == NULL)
			continue;
		if (max == NULL) {
			maxv = v;
			max = src;
			continue;
		}
		int rc;
		rc = sf_compare(i->r->scheme,
		                sv_pointer(maxv), sv_size(maxv),
		                sv_pointer(v), sv_size(v));
		switch (rc) {
		case  0:
			/*
			assert(sv_lsn(v) < sv_lsn(maxv));
			*/
			src->dup = 1;
			break;
		case -1:
			sv_mergeiter_dupreset(i, src);
			maxv = v;
			max = src;
			break;
		}
	}
	if (ssunlikely(max == NULL))
		return;
	i->v = max;
}

static inline void
sv_mergeiter_next(ssiter *it)
{
	svmergeiter *im = (svmergeiter*)it->priv;
	switch (im->order) {
	case SS_GT:
	case SS_GTE:
		sv_mergeiter_gt(im);
		break;
	case SS_LT:
	case SS_LTE:
		sv_mergeiter_lt(im);
		break;
	default: assert(0);
	}
}

static inline int
sv_mergeiter_open(ssiter *i, sr *r, svmerge *m, ssorder o)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	im->merge = m;
	im->r     = r;
	im->order = o;
	im->src   = (svmergesrc*)(im->merge->buf.s);
	im->end   = (svmergesrc*)(im->merge->buf.p);
	im->v     = NULL;
	sv_mergeiter_next(i);
	return 0;
}

static inline void
sv_mergeiter_close(ssiter *i ssunused)
{ }

static inline int
sv_mergeiter_has(ssiter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	return im->v != NULL;
}

static inline void*
sv_mergeiter_of(ssiter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	if (ssunlikely(im->v == NULL))
		return NULL;
	return ss_iteratorof(im->v->i);
}

static inline uint32_t
sv_mergeisdup(ssiter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	assert(im->v != NULL);
	if (im->v->dup)
		return SVDUP;
	return 0;
}

extern ssiterif sv_mergeiter;

#endif
