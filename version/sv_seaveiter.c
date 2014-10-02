
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

typedef struct svseaveiter svseaveiter;

struct svseaveiter {
	sriter *merge;
	uint64_t limit; 
	uint64_t size;
	uint32_t sizev;
	uint32_t totalkv;
	uint64_t lsvn;
	int next;
	sv *v;
} srpacked;

static void
sv_seaveiter_init(sriter *i)
{
	assert(sizeof(svseaveiter) <= sizeof(i->priv));
	svseaveiter *im = (svseaveiter*)i->priv;
	memset(im, 0, sizeof(*im));
}

static void sv_seaveiter_next(sriter*);

static int
sv_seaveiter_open(sriter *i, va_list args)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	im->merge = va_arg(args, sriter*);
	im->limit = va_arg(args, uint64_t);
	im->sizev = va_arg(args, uint32_t);
	im->lsvn  = va_arg(args, uint64_t);
	sv_seaveiter_next(i);
	return 0;
}

static void
sv_seaveiter_close(sriter *i srunused)
{ }

static int
sv_seaveiter_has(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	return im->v != NULL;
}

static void*
sv_seaveiter_of(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return im->v;
}

static void
sv_seaveiter_next(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	if (im->next)
		sr_iternext(im->merge);
	im->next = 0;
	im->v = NULL;
	while (sr_iterhas(im->merge))
	{
		sv *v = sr_iterof(im->merge);
		uint8_t dup = (svflags(v) & SVDUP) > 0;
		if (im->size >= im->limit) {
			if (! dup)
				break;
		}
		int kv = svkeysize(v) + svvaluesize(v);
		if (srunlikely(dup)) {
			if (svlsn(v) < im->lsvn) {
				im->totalkv += kv;
				sr_iternext(im->merge);
				continue;
			}
		} else {
			uint8_t del = (svflags(v) & SVDELETE) > 0;
			if (srunlikely(del && (svlsn(v) < im->lsvn))) {
				im->totalkv += kv;
				sr_iternext(im->merge);
				continue;
			}
			im->size += im->sizev + kv;
			im->totalkv += kv;
		}
		im->v = v;
		im->next = 1;
		break;
	}
}

sriterif sv_seaveiter =
{
	.init    = sv_seaveiter_init,
	.open    = sv_seaveiter_open,
	.close   = sv_seaveiter_close,
	.has     = sv_seaveiter_has,
	.of      = sv_seaveiter_of,
	.next    = sv_seaveiter_next
};

int sv_seaveiter_resume(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	im->v    = sr_iterof(im->merge);
	if (srunlikely(im->v == NULL))
		return 0;
	im->next = 1;
	im->size = im->sizev + svkeysize(im->v) +
	           svvaluesize(im->v);
	return 1;
}

uint32_t sv_seaveiter_totalkv(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	return im->totalkv;
}
