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
	sriter *merge;
	uint64_t limit;
	uint64_t size;
	uint32_t sizev;
	uint64_t vlsn;
	int save_delete;
	int next;
	uint64_t prevlsn;
	sv *v;
} srpacked;

static inline void
sv_writeiter_next(sriter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	if (im->next)
		sr_iternext(sv_mergeiter, im->merge);
	im->next = 0;
	im->v = NULL;
	for (; sr_iterhas(sv_mergeiter, im->merge); sr_iternext(sv_mergeiter, im->merge))
	{
		sv *v = sr_iterof(sv_mergeiter, im->merge);
		int dup = (sv_flags(v) & SVDUP) | sv_mergeisdup(im->merge);
		if (im->size >= im->limit) {
			if (! dup)
				break;
		}
		uint64_t lsn = sv_lsn(v);
		int kv = sv_keysize(v) + sv_valuesize(v);
		if (srunlikely(dup)) {
			/* keep atleast one visible version for <= vlsn */
			if (im->prevlsn <= im->vlsn)
				continue;
		} else {
			/* branched or stray deletes */
			if (! im->save_delete) {
				int del = (sv_flags(v) & SVDELETE) > 0;
				if (srunlikely(del && (lsn <= im->vlsn))) {
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
sv_writeiter_open(sriter *i, sriter *iterator, uint64_t limit,
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
sv_writeiter_close(sriter *i srunused)
{ }

static inline int
sv_writeiter_has(sriter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	return im->v != NULL;
}

static inline void*
sv_writeiter_of(sriter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return im->v;
}

static inline int
sv_writeiter_resume(sriter *i)
{
	svwriteiter *im = (svwriteiter*)i->priv;
	im->v    = sr_iterof(sv_mergeiter, im->merge);
	if (srunlikely(im->v == NULL))
		return 0;
	im->next = 1;
	im->size = im->sizev + sv_keysize(im->v) +
	           sv_valuesize(im->v);
	return 1;
}

extern sriterif sv_writeiter;

#endif
