
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>

typedef struct sditer sditer;

struct sditer {
	int validate;
	sdindex *index;
	char *start, *end;
	char *page;
	sdpage pagev;
	uint32_t pos;
	sdv *dv;
	sv v;
} srpacked;

static void
sd_iterinit(sriter *i)
{
	assert(sizeof(sditer) <= sizeof(i->priv));
	sditer *ii = (sditer*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static int sd_iternextpage(sriter*);

static int
sd_iteropen(sriter *i, va_list args)
{
	sditer *ii = (sditer*)i->priv;
	ii->index    = va_arg(args, sdindex*);
	ii->start    = va_arg(args, char*);
	ii->validate = va_arg(args, int);
	return sd_iternextpage(i);
}

static void
sd_iterclose(sriter *i srunused)
{
	sditer *ii = (sditer*)i->priv;
	(void)ii;
}

static int
sd_iterhas(sriter *i)
{
	sditer *ii = (sditer*)i->priv;
	return ii->page != NULL;
}

static void*
sd_iterof(sriter *i)
{
	sditer *ii = (sditer*)i->priv;
	if (srunlikely(ii->page == NULL))
		return NULL;
	assert(ii->dv != NULL);
	assert(ii->v.v  == ii->dv);
	assert(ii->v.arg == ii->pagev.h);
	return &ii->v;
}

static inline int
sd_iternextpage(sriter *it)
{
	sditer *i = (sditer*)it->priv;
	char *page = NULL;
	if (srunlikely(i->page == NULL))
	{
		sdindexheader *h = i->index->h;
		page = i->start + h->offset + sd_indexsize(i->index->h);
		i->end = page + h->total;
	} else {
		page = i->page + sizeof(sdpageheader) + i->pagev.h->size;
	}
	if (srunlikely(page >= i->end)) {
		i->page = NULL;
		return 0;
	}
	i->page = page;
	if (i->validate) {
		sdpageheader *h = (sdpageheader*)i->page;
		uint32_t crc = sr_crcs(it->r->crc, h, sizeof(sdpageheader), 0);
		if (srunlikely(crc != h->crc)) {
			i->page = NULL;
			sr_malfunction(it->r->e, "%s", "bad page header crc");
			return -1;
		}
	}
	sd_pageinit(&i->pagev, (void*)i->page);
	i->pos = 0;
	if (srunlikely(i->pagev.h->count == 0)) {
		i->page = NULL;
		i->dv = NULL;
		return 0;
	}
	i->dv = sd_pagev(&i->pagev, 0);
	svinit(&i->v, &sd_vif, i->dv, i->pagev.h);
	return 1;
}

static void
sd_iternext(sriter *i)
{
	sditer *ii = (sditer*)i->priv;
	if (srunlikely(ii->page == NULL))
		return;
	ii->pos++;
	if (srlikely(ii->pos < ii->pagev.h->count)) {
		ii->dv = sd_pagev(&ii->pagev, ii->pos);
		svinit(&ii->v, &sd_vif, ii->dv, ii->pagev.h);
	} else {
		ii->dv = NULL;
		sd_iternextpage(i);
	}
}

sriterif sd_iter =
{
	.init    = sd_iterinit,
	.open    = sd_iteropen,
	.close   = sd_iterclose,
	.has     = sd_iterhas,
	.of      = sd_iterof,
	.next    = sd_iternext
};
