
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

typedef struct svsiftiter svsiftiter;

struct svsiftiter {
	sriter *merge;
	uint64_t limit; 
	uint64_t size;
	uint32_t sizev;
	uint32_t total; /* kv */
	uint64_t vlsn;
	int save_delete;
	int next;
	sv *v;
} srpacked;

static void
sv_siftiter_init(sriter *i)
{
	assert(sizeof(svsiftiter) <= sizeof(i->priv));
	svsiftiter *im = (svsiftiter*)i->priv;
	memset(im, 0, sizeof(*im));
}

static void sv_siftiter_next(sriter*);

static int
sv_siftiter_open(sriter *i, va_list args)
{
	svsiftiter *im = (svsiftiter*)i->priv;
	im->merge = va_arg(args, sriter*);
	im->limit = va_arg(args, uint64_t);
	im->sizev = va_arg(args, uint32_t);
	im->vlsn  = va_arg(args, uint64_t);
	im->save_delete = va_arg(args, int);
	sv_siftiter_next(i);
	return 0;
}

static void
sv_siftiter_close(sriter *i srunused)
{ }

static int
sv_siftiter_has(sriter *i)
{
	svsiftiter *im = (svsiftiter*)i->priv;
	return im->v != NULL;
}

static void*
sv_siftiter_of(sriter *i)
{
	svsiftiter *im = (svsiftiter*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return im->v;
}

static void
sv_siftiter_next(sriter *i)
{
	svsiftiter *im = (svsiftiter*)i->priv;
	if (im->next)
		sr_iternext(im->merge);
	im->next = 0;
	im->v = NULL;
	for (;sr_iterhas(im->merge); sr_iternext(im->merge))
	{
		sv *v = sr_iterof(im->merge);
		uint32_t flags = svflags(v);
		uint8_t dup = (flags & SVDUP) > 0;
		if (im->size >= im->limit) {
			if (! dup)
				break;
		}
		int kv = svkeysize(v) + svvaluesize(v);
		im->total += kv;
		if (srunlikely(dup)) {
			if (svlsn(v) < im->vlsn)
				continue;
		} else {
			if (! im->save_delete) {
				uint8_t del = (flags & SVDELETE) > 0;
				if (srunlikely(del && (svlsn(v) < im->vlsn)))
					continue;
			}
			im->size += im->sizev + kv;
		}
		im->v = v;
		im->next = 1;
		break;
	}
}

sriterif sv_siftiter =
{
	.init    = sv_siftiter_init,
	.open    = sv_siftiter_open,
	.close   = sv_siftiter_close,
	.has     = sv_siftiter_has,
	.of      = sv_siftiter_of,
	.next    = sv_siftiter_next
};

int sv_siftiter_resume(sriter *i)
{
	svsiftiter *im = (svsiftiter*)i->priv;
	im->v    = sr_iterof(im->merge);
	if (srunlikely(im->v == NULL))
		return 0;
	im->next = 1;
	im->size = im->sizev + svkeysize(im->v) +
	           svvaluesize(im->v);
	return 1;
}

uint32_t sv_siftiter_total(sriter *i)
{
	svsiftiter *im = (svsiftiter*)i->priv;
	return im->total;
}
