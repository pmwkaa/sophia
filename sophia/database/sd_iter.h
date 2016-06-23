#ifndef SD_ITER_H_
#define SD_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sditer sditer;

struct sditer {
	int         validate;
	int         compression;
	ssfilterif *compression_if;
	ssbuf      *compression_buf;
	sdindex    *index;
	char       *start, *end;
	char       *page;
	char       *pagesrc;
	sdpage      pagev;
	uint32_t    pos;
	char       *v;
	sr         *r;
} sspacked;

static inline void
sd_iterresult(sditer *i, int pos)
{
	i->v = sd_pagepointer(&i->pagev, i->r, pos);
}

static inline int
sd_iternextpage(sditer *i)
{
	char *page = NULL;
	if (ssunlikely(i->page == NULL))
	{
		sdindexheader *h = i->index->h;
		page = i->start + (i->index->h->offset - i->index->h->total);
		i->end = page + h->total;
	} else {
		page = i->pagesrc + sizeof(sdpageheader) + i->pagev.h->size;
	}
	if (ssunlikely(page >= i->end)) {
		i->page = NULL;
		return 0;
	}
	i->pagesrc = page;
	i->page = i->pagesrc;

	/* decompression */
	if (i->compression) {
		ss_bufreset(i->compression_buf);

		/* prepare decompression buffer */
		sdpageheader *h = (sdpageheader*)i->page;
		int rc = ss_bufensure(i->compression_buf, i->r->a,
		                      h->sizeorigin + sizeof(sdpageheader));
		if (ssunlikely(rc == -1)) {
			i->page = NULL;
			return sr_oom_malfunction(i->r->e);
		}

		/* copy page header */
		memcpy(i->compression_buf->s, i->page, sizeof(sdpageheader));
		ss_bufadvance(i->compression_buf, sizeof(sdpageheader));

		/* decompression */
		ssfilter f;
		rc = ss_filterinit(&f, (ssfilterif*)i->compression_if, i->r->a, SS_FOUTPUT);
		if (ssunlikely(rc == -1)) {
			i->page = NULL;
			sr_malfunction(i->r->e, "%s", "page decompression error");
			return -1;
		}
		rc = ss_filternext(&f, i->compression_buf,
		                   i->page + sizeof(sdpageheader), h->size);
		if (ssunlikely(rc == -1)) {
			ss_filterfree(&f);
			i->page = NULL;
			sr_malfunction(i->r->e, "%s", "page decompression error");
			return -1;
		}
		ss_filterfree(&f);

		/* switch to decompressed page */
		i->page = i->compression_buf->s;
	}

	/* checksum */
	if (i->validate) {
		sdpageheader *h = (sdpageheader*)i->page;
		uint32_t crc = ss_crcs(i->r->crc, h, sizeof(sdpageheader), 0);
		if (ssunlikely(crc != h->crc)) {
			i->page = NULL;
			sr_malfunction(i->r->e, "%s", "bad page header crc");
			return -1;
		}
	}
	sd_pageinit(&i->pagev, (void*)i->page);
	i->pos = 0;
	if (ssunlikely(i->pagev.h->count == 0)) {
		i->page = NULL;
		return 0;
	}
	sd_iterresult(i, 0);
	return 1;
}

static inline int
sd_iter_open(ssiter *i, sr *r, sdindex *index, char *start, int validate,
             int compression,
             ssbuf *compression_buf)
{
	sditer *ii = (sditer*)i->priv;
	ii->r               = r;
	ii->index           = index;
	ii->start           = start;
	ii->end             = NULL;
	ii->page            = NULL;
	ii->pagesrc         = NULL;
	ii->pos             = 0;
	ii->validate        = validate;
	ii->compression     = compression;
	ii->compression_buf = compression_buf;
	ii->v               = NULL;
	return sd_iternextpage(ii);
}

static inline void
sd_iter_close(ssiter *i ssunused)
{
	sditer *ii = (sditer*)i->priv;
	(void)ii;
}

static inline int
sd_iter_has(ssiter *i)
{
	sditer *ii = (sditer*)i->priv;
	return ii->page != NULL;
}

static inline void*
sd_iter_of(ssiter *i)
{
	sditer *ii = (sditer*)i->priv;
	if (ssunlikely(ii->page == NULL))
		return NULL;
	return ii->v;
}

static inline void
sd_iter_next(ssiter *i)
{
	sditer *ii = (sditer*)i->priv;
	if (ssunlikely(ii->page == NULL))
		return;
	ii->pos++;
	if (sslikely(ii->pos < ii->pagev.h->count)) {
		sd_iterresult(ii, ii->pos);
	} else {
		sd_iternextpage(ii);
	}
}

extern ssiterif sd_iter;

#endif
