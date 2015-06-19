#ifndef SV_READITER_H_
#define SV_READITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svreaditer svreaditer;

struct svreaditer {
	ssiter *merge;
	uint64_t vlsn;
	int next;
	int nextdup;
	sv *v;
} sspacked;

static inline void
sv_readiter_next(ssiter *i)
{
	svreaditer *im = (svreaditer*)i->priv;
	if (im->next)
		ss_iternext(sv_mergeiter, im->merge);
	im->next = 0;
	im->v = NULL;
	for (; ss_iterhas(sv_mergeiter, im->merge); ss_iternext(sv_mergeiter, im->merge))
	{
		sv *v = ss_iterof(sv_mergeiter, im->merge);
		/* distinguish duplicates between merge
		 * streams only */
		int dup = sv_mergeisdup(im->merge);
		if (im->nextdup) {
			if (dup)
				continue;
			else
				im->nextdup = 0;
		}
		/* assume that iteration sources are
		 * version aware */
		assert(sv_lsn(v) <= im->vlsn);
		im->nextdup = 1;
		if (ssunlikely(sv_is(v, SVDELETE)))
			continue;
		/* ignore stray updates */
		if (ssunlikely(sv_is(v, SVUPDATE)))
			continue;
		im->v = v;
		im->next = 1;
		break;
	}
}

static inline int
sv_readiter_open(ssiter *i, ssiter *iterator, uint64_t vlsn)
{
	svreaditer *im = (svreaditer*)i->priv;
	im->merge = iterator;
	im->vlsn  = vlsn;
	assert(im->merge->vif == &sv_mergeiter);
	im->v = NULL;
	im->next = 0;
	im->nextdup = 0;
	/* iteration can start from duplicate */
	sv_readiter_next(i);
	return 0;
}

static inline void
sv_readiter_close(ssiter *i ssunused)
{ }

static inline int
sv_readiter_has(ssiter *i)
{
	svreaditer *im = (svreaditer*)i->priv;
	return im->v != NULL;
}

static inline void*
sv_readiter_of(ssiter *i)
{
	svreaditer *im = (svreaditer*)i->priv;
	if (ssunlikely(im->v == NULL))
		return NULL;
	return im->v;
}

extern ssiterif sv_readiter;

#endif
