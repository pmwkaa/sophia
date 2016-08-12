#ifndef SV_WRITEITER_H_
#define SV_WRITEITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svwriteiter svwriteiter;

struct svwriteiter {
	uint64_t  vlsn;
	uint64_t  limit;
	uint64_t  size;
	uint32_t  sizev;
	uint32_t  expire;
	uint32_t  now;
	int       next;
	int       upsert;
	uint64_t  prevlsn;
	int       vdup;
	char     *v;
	svupsert *u;
	ssiter   *merge;
	sr       *r;
} sspacked;

static inline int
sv_writeiter_upsert(svwriteiter *i)
{
	/* apply upsert only on statements which are the latest or
	 * ready to be garbage-collected */
	sv_upsertreset(i->u);

	/* upsert begin */
	char *v = ss_iterof(sv_mergeiter, i->merge);
	assert(v != NULL);
	assert(sf_flags(i->r->scheme, v) & SVUPSERT);
	assert(sf_lsn(i->r->scheme, v) <= i->vlsn);
	int rc = sv_upsertpush(i->u, i->r, v);
	if (ssunlikely(rc == -1))
		return -1;
	ss_iternext(sv_mergeiter, i->merge);

	/* iterate over upsert statements */
	int last_non_upd = 0;
	for (; ss_iterhas(sv_mergeiter, i->merge); ss_iternext(sv_mergeiter, i->merge))
	{
		v = ss_iterof(sv_mergeiter, i->merge);
		int flags = sf_flags(i->r->scheme, v);
		int dup = sf_flagsequ(flags, SVDUP) || sv_mergeisdup(i->merge);
		if (! dup)
			break;
		/* stop forming upserts on a second non-upsert stmt,
		 * but continue to iterate stream */
		if (last_non_upd)
			continue;
		last_non_upd = ! sf_flagsequ(flags, SVUPSERT);
		int rc = sv_upsertpush(i->u, i->r, v);
		if (ssunlikely(rc == -1))
			return -1;
	}

	/* upsert */
	rc = sv_upsert(i->u, i->r);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

static inline void
sv_writeiter_next(ssiter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	if (im->next)
		ss_iternext(sv_mergeiter, im->merge);
	im->next = 0;
	im->v = NULL;
	im->vdup = 0;

	for (; ss_iterhas(sv_mergeiter, im->merge); ss_iternext(sv_mergeiter, im->merge))
	{
		char *v = ss_iterof(sv_mergeiter, im->merge);
		/* expiration logic */
		if (im->expire > 0) {
			uint32_t timestamp = sf_ttl(im->r->scheme, v);
			if ((im->now - timestamp) >= im->expire)
				 continue;
		}
		uint64_t lsn = sf_lsn(im->r->scheme, v);
		int flags = sf_flags(im->r->scheme, v);
		int dup = sf_flagsequ(flags, SVDUP) || sv_mergeisdup(im->merge);
		if (im->size >= im->limit) {
			if (! dup)
				break;
		}

		if (ssunlikely(dup)) {
			/* keep atleast one visible version for <= vlsn */
			if (im->prevlsn <= im->vlsn) {
				if (im->upsert) {
					im->upsert = sf_flagsequ(flags, SVUPSERT);
				} else {
					continue;
				}
			}
		} else {
			im->upsert = 0;
			/* delete (stray) */
			int del = sf_flagsequ(flags, SVDELETE);
			if (ssunlikely(del && (lsn <= im->vlsn))) {
				im->prevlsn = lsn;
				continue;
			}
			im->size += im->sizev + sf_size(im->r->scheme, v);
			/* upsert (track first statement start) */
			if (sf_flagsequ(flags, SVUPSERT))
				im->upsert = 1;
		}

		/* upsert */
		if (sf_flagsequ(flags, SVUPSERT)) {
			if (lsn <= im->vlsn) {
				int rc;
				rc = sv_writeiter_upsert(im);
				if (ssunlikely(rc == -1))
					return;
				im->upsert = 0;
				im->prevlsn = lsn;
				im->v = im->u->result;
				im->vdup = dup;
				im->next = 0;
				break;
			}
		}

		im->prevlsn = lsn;
		im->v = v;
		im->vdup = dup;
		im->next = 1;
		break;
	}
}

static inline int
sv_writeiter_open(ssiter *i, sr *r, ssiter *merge, svupsert *u,
                  uint64_t limit,
                  uint32_t sizev,
                  uint32_t expire,
                  uint32_t timestamp,
                  uint64_t vlsn)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	im->u       = u;
	im->r       = r;
	im->limit   = limit;
	im->size    = 0;
	im->sizev   = sizev;
	im->expire  = expire;
	im->now     = timestamp;
	im->vlsn    = vlsn;
	im->next    = 0;
	im->prevlsn = 0;
	im->v       = NULL;
	im->vdup    = 0;
	im->upsert  = 0;
	im->merge   = merge;
	assert(im->merge->vif == &sv_mergeiter);
	sv_writeiter_next(i);
	return 0;
}

static inline void
sv_writeiter_close(ssiter *i ssunused)
{ }

static inline int
sv_writeiter_has(ssiter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	return im->v != NULL;
}

static inline void*
sv_writeiter_of(ssiter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	if (ssunlikely(im->v == NULL))
		return NULL;
	return im->v;
}

static inline int
sv_writeiter_resume(ssiter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	im->v       = ss_iterof(sv_mergeiter, im->merge);
	if (ssunlikely(im->v == NULL))
		return 0;
	im->vdup    = sf_is(im->r->scheme, im->v, SVDUP) || sv_mergeisdup(im->merge);
	im->prevlsn = sf_lsn(im->r->scheme, im->v);
	im->next    = 1;
	im->upsert  = 0;
	im->size    = im->sizev + sf_size(im->r->scheme, im->v);
	return 1;
}

static inline int
sv_writeiter_is_duplicate(ssiter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	assert(im->v != NULL);
	return im->vdup;
}

extern ssiterif sv_writeiter;

#endif
