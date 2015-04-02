
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

typedef struct svreaditer svreaditer;

struct svreaditer {
	sriter *merge;
	uint64_t vlsn;
	int next;
	int nextdup;
	sv *v;
} srpacked;

static void sv_readiter_next(sriter*);

static int
sv_readiter_open(sriter *i, va_list args)
{
	svreaditer *im = (svreaditer*)i->priv;
	im->merge = va_arg(args, sriter*);
	im->vlsn  = va_arg(args, uint64_t);
	assert(im->merge->i == &sv_mergeiter);
	im->v = NULL;
	im->next = 0;
	im->nextdup = 0;
	/* iteration can start from duplicate */
	sv_readiter_next(i);
	return 0;
}

static void
sv_readiter_close(sriter *i srunused)
{ }

static int
sv_readiter_has(sriter *i)
{
	svreaditer *im = (svreaditer*)i->priv;
	return im->v != NULL;
}

static void*
sv_readiter_of(sriter *i)
{
	svreaditer *im = (svreaditer*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return im->v;
}

static void
sv_readiter_next(sriter *i)
{
	svreaditer *im = (svreaditer*)i->priv;
	if (im->next)
		sr_iternext(im->merge);
	im->next = 0;
	im->v = NULL;
	for (; sr_iterhas(im->merge); sr_iternext(im->merge))
	{
		sv *v = sr_iterof(im->merge);
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
		int del = (sv_flags(v) & SVDELETE) > 0;
		if (srunlikely(del))
			continue;
		im->v = v;
		im->next = 1;
		break;
	}
}

sriterif sv_readiter =
{
	.open    = sv_readiter_open,
	.close   = sv_readiter_close,
	.has     = sv_readiter_has,
	.of      = sv_readiter_of,
	.next    = sv_readiter_next
};
