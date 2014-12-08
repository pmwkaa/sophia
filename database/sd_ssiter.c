
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

typedef struct sdssiter sdssiter;

struct sdssiter {
	int validate;
	srbuf *buf;
	sdssrecord *v;
} srpacked;

static void
sd_ssiterinit(sriter *i)
{
	assert(sizeof(sdssiter) <= sizeof(i->priv));
	sdssiter *si = (sdssiter*)i->priv;
	memset(si, 0, sizeof(*si));
}

static int
sd_ssiternext_do(sriter *it)
{
	sdssiter *i = (sdssiter*)it->priv;
	if (i->v == NULL) {
		if (sr_bufused(i->buf) == 0)
			return 0;
		sdssheader *h = (sdssheader*)i->buf->s;
		if (i->validate) {
			uint32_t crc = sr_crcs(h, sr_bufused(i->buf), 0);
			if (srunlikely(crc != h->crc)) {
				i->v = NULL;
				sr_error(it->r->e, "%s", "bad snapshot header crc");
				return -1;
			}
		}
		if (h->count == 0)
			return 0;
		i->v = (sdssrecord*)(i->buf->s + sizeof(sdssheader));
	} else {
		i->v = (sdssrecord*)((char*)i->v + sizeof(sdssrecord) + i->v->namelen);
	}
	if (srunlikely((char*)i->v > i->buf->p)) {
		i->v = NULL;
		return -1;
	}
	if (srunlikely((char*)i->v == i->buf->p)) {
		i->v = NULL;
		return  0;
	}
	return 1;
}

static int
sd_ssiteropen(sriter *i, va_list args)
{
	sdssiter *si = (sdssiter*)i->priv;
	si->buf      = va_arg(args, srbuf*);
	si->validate = va_arg(args, int);
	return sd_ssiternext_do(i);
}

static void
sd_ssiterclose(sriter *i srunused)
{
	sdssiter *si = (sdssiter*)i->priv;
	(void)si;
}

static int
sd_ssiterhas(sriter *i)
{
	sdssiter *si = (sdssiter*)i->priv;
	return si->v != NULL;
}

static void*
sd_ssiterof(sriter *i)
{
	sdssiter *si = (sdssiter*)i->priv;
	return si->v;
}

static void
sd_ssiternext(sriter *i)
{
	sd_ssiternext_do(i);
}

sriterif sd_ssiter =
{
	.init    = sd_ssiterinit,
	.open    = sd_ssiteropen,
	.close   = sd_ssiterclose,
	.has     = sd_ssiterhas,
	.of      = sd_ssiterof,
	.next    = sd_ssiternext
};
