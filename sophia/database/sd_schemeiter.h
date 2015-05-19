#ifndef SD_SCHEMEITER_H_
#define SD_SCHEMEITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdschemeiter sdschemeiter;

struct sdschemeiter {
	sdscheme *c;
	char *p;
} srpacked;

static inline int
sd_schemeiter_open(sriter *i, sdscheme *c, int validate)
{
	sdschemeiter *ci = (sdschemeiter*)i->priv;
	ci->c = c;
	ci->p = NULL;
	if (validate) {
		sdschemeheader *h = (sdschemeheader*)c->buf.s;
		uint32_t crc = sr_crcs(i->r->crc, h, sr_bufused(&c->buf), 0);
		if (h->crc != crc) {
			sr_malfunction(i->r->e, "%s", "scheme file corrupted");
			return -1;
		}
	}
	ci->p = c->buf.s + sizeof(sdschemeheader);
	return 0;
}

static inline void
sd_schemeiter_close(sriter *i srunused)
{
	sdschemeiter *ci = (sdschemeiter*)i->priv;
	(void)ci;
}

static inline int
sd_schemeiter_has(sriter *i)
{
	sdschemeiter *ci = (sdschemeiter*)i->priv;
	return ci->p < ci->c->buf.p;
}

static inline void*
sd_schemeiter_of(sriter *i)
{
	sdschemeiter *ci = (sdschemeiter*)i->priv;
	if (srunlikely(ci->p >= ci->c->buf.p))
		return NULL;
	return ci->p;
}

static inline void
sd_schemeiter_next(sriter *i)
{
	sdschemeiter *ci = (sdschemeiter*)i->priv;
	if (srunlikely(ci->p >= ci->c->buf.p))
		return;
	sdschemeopt *o = (sdschemeopt*)ci->p;
	ci->p = (char*)o + sizeof(sdschemeopt) + o->size;
}

extern sriterif sd_schemeiter;

#endif
