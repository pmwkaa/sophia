
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

typedef struct svcmpiter svcmpiter;

#define SV_CMPITER_A 1
#define SV_CMPITER_B 2

struct svcmpiter {
	int mask;
	sriter *a, *b;
	sriter *v;
} srpacked;

static void
sv_cmpiter_init(sriter *i)
{
	assert(sizeof(svcmpiter) <= sizeof(i->priv));
	svcmpiter *im = (svcmpiter*)i->priv;
	memset(im, 0, sizeof(*im));
}

static void sv_cmpiter_next(sriter*);

static int
sv_cmpiter_open(sriter *i, va_list args)
{
	svcmpiter *im = (svcmpiter*)i->priv;
	im->a = va_arg(args, sriter*);
	im->b = va_arg(args, sriter*);
	sv_cmpiter_next(i);
	return 0;
}

static void
sv_cmpiter_close(sriter *i srunused)
{ }

static int
sv_cmpiter_has(sriter *i)
{
	svcmpiter *im = (svcmpiter*)i->priv;
	return im->v != NULL;
}

static void*
sv_cmpiter_of(sriter *i)
{
	svcmpiter *im = (svcmpiter*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return sr_iterof(im->v);
}

static void
sv_cmpiter_next(sriter *i)
{
	svcmpiter *im = (svcmpiter*)i->priv;
	if (im->mask & SV_CMPITER_A)
		sr_iternext(im->a);
	if (im->mask & SV_CMPITER_B)
		sr_iternext(im->b);
	im->mask = 0;
	sv *a = sr_iterof(im->a);
	sv *b = sr_iterof(im->b);
	if (a && b) {
		assert(a->v != NULL);
		assert(b->v != NULL);
		int rc = svcompare(a, b, i->r->cmp);
		switch (rc) {
		case  0:
			im->v = im->a;
			im->mask = SV_CMPITER_A;
			svsetdup(b);
			break;
		case -1:
			im->v = im->a;
			im->mask = SV_CMPITER_A;
			break;
		case  1:
			im->v = im->b;
			im->mask = SV_CMPITER_B;
			break;
		}
		return;
	}
	if (a) {
		im->v = im->a;
		im->mask = SV_CMPITER_A;
		return;
	}
	if (b) {
		im->v = im->b;
		im->mask = SV_CMPITER_B;
		return;
	}
	im->v = NULL;
}

sriterif sv_cmpiter =
{
	.init    = sv_cmpiter_init,
	.open    = sv_cmpiter_open,
	.close   = sv_cmpiter_close,
	.has     = sv_cmpiter_has,
	.of      = sv_cmpiter_of,
	.next    = sv_cmpiter_next
};
