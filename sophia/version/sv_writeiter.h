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
	ssiter *merge;
	uint64_t limit;
	uint64_t size;
	uint32_t sizev;
	uint64_t vlsn;
	int save_delete;
	int next;
	uint64_t prevlsn;
	sv *v;
} sspacked;

static inline void
sv_writeiter_next(ssiter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	if (im->next)
		ss_iternext(sv_mergeiter, im->merge);
	im->next = 0;
	im->v = NULL;
	for (; ss_iterhas(sv_mergeiter, im->merge); ss_iternext(sv_mergeiter, im->merge))
	{
		sv *v = ss_iterof(sv_mergeiter, im->merge);
		int dup = sv_is(v, SVDUP) || sv_mergeisdup(im->merge);
		if (im->size >= im->limit) {
			if (! dup)
				break;
		}
		uint64_t lsn = sv_lsn(v);
		int kv = sv_size(v);
		if (ssunlikely(dup)) {
			/* keep atleast one visible version for <= vlsn */
			if (im->prevlsn <= im->vlsn)
				continue;
		} else {
			/* branched or stray deletes */
			if (! im->save_delete) {
				int del = sv_is(v, SVDELETE);
				if (ssunlikely(del && (lsn <= im->vlsn))) {
					im->prevlsn = lsn;
					continue;
				}
			}
			im->size += im->sizev + kv;
		}
		im->prevlsn = lsn;
		im->v = v;
		im->next = 1;
		break;
	}
}

static inline int
sv_writeiter_open(ssiter *i, ssiter *iterator, uint64_t limit,
                  uint32_t sizev, uint64_t vlsn,
                  int save_delete)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	im->merge = iterator;
	im->limit = limit;
	im->size  = 0;
	im->sizev = sizev;
	im->vlsn  = vlsn;;
	im->save_delete = save_delete;
	assert(im->merge->vif == &sv_mergeiter);
	im->next  = 0;
	im->prevlsn  = 0;
	im->v = NULL;
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
	im->v    = ss_iterof(sv_mergeiter, im->merge);
	if (ssunlikely(im->v == NULL))
		return 0;
	im->next = 1;
	im->size = im->sizev + sv_size(im->v);
	return 1;
}

extern ssiterif sv_writeiter;

#endif
