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
	ssiter   *merge;
	uint64_t  vlsn;
	int       next;
	int       nextdup;
	int       save_delete;
	svupsert *u;
	sr       *r;
	char     *v;
} sspacked;

static inline int
sv_readiter_upsert(svreaditer *i)
{
	sv_upsertreset(i->u);
	/* upsert begin */
	char *v = ss_iterof(sv_mergeiter, i->merge);
	assert(v != NULL);
	assert(sf_flags(i->r->scheme, v) & SVUPSERT);
	int rc = sv_upsertpush(i->u, i->r, v);
	if (ssunlikely(rc == -1))
		return -1;
	ss_iternext(sv_mergeiter, i->merge);
	/* iterate over upsert statements */
	int skip = 0;
	for (; ss_iterhas(sv_mergeiter, i->merge); ss_iternext(sv_mergeiter, i->merge))
	{
		v = ss_iterof(sv_mergeiter, i->merge);
		int dup = sf_is(i->r->scheme, v, SVDUP) || sv_mergeisdup(i->merge);
		if (! dup)
			break;
		if (skip)
			continue;
		int rc = sv_upsertpush(i->u, i->r, v);
		if (ssunlikely(rc == -1))
			return -1;
		if (! (sf_flags(i->r->scheme, v) & SVUPSERT))
			skip = 1;
	}
	/* upsert */
	rc = sv_upsert(i->u, i->r);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

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
		char *v = ss_iterof(sv_mergeiter, im->merge);
		int dup = sf_is(im->r->scheme, v, SVDUP) || sv_mergeisdup(im->merge);
		if (im->nextdup) {
			if (dup)
				continue;
			im->nextdup = 0;
		}
		/* skip version out of visible range */
		if (sf_lsn(im->r->scheme, v) > im->vlsn) {
			continue;
		}
		im->nextdup = 1;
		if (ssunlikely(!im->save_delete && sf_is(im->r->scheme, v, SVDELETE)))
			continue;
		if (ssunlikely(sf_is(im->r->scheme, v, SVUPSERT))) {
			int rc = sv_readiter_upsert(im);
			if (ssunlikely(rc == -1))
				return;
			im->v = im->u->result;
			im->next = 0;
		} else {
			im->v = v;
			im->next = 1;
		}
		break;
	}
}

static inline void
sv_readiter_forward(ssiter *i)
{
	svreaditer *im = (svreaditer*)i->priv;
	if (im->next)
		ss_iternext(sv_mergeiter, im->merge);
	im->next = 0;
	im->v = NULL;
	for (; ss_iterhas(sv_mergeiter, im->merge); ss_iternext(sv_mergeiter, im->merge))
	{
		char *v = ss_iterof(sv_mergeiter, im->merge);
		int dup = sf_is(im->r->scheme, v, SVDUP) || sv_mergeisdup(im->merge);
		if (dup)
			continue;
		im->next = 0;
		im->v = v;
		break;
	}
}

static inline int
sv_readiter_open(ssiter *i, sr *r, ssiter *iterator, svupsert *u,
                 uint64_t vlsn, int save_delete)
{
	svreaditer *im  = (svreaditer*)i->priv;
	im->r           = r;
	im->u           = u;
	im->vlsn        = vlsn;
	im->v           = NULL;
	im->next        = 0;
	im->nextdup     = 0;
	im->save_delete = save_delete;
	im->merge       = iterator;
	assert(im->merge->vif == &sv_mergeiter);
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
