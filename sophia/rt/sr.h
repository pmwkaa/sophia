#ifndef SR_H_
#define SR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sr sr;

struct sr {
	srerror *e;
	srfmtstorage fmt_storage;
	srfmt fmt;
	srscheme *scheme;
	srseq *seq;
	sra *a;
	srinjection *i;
	void *compression;
	srcrcf crc;
};

static inline void
sr_init(sr *r,
        srerror *e,
        sra *a,
        srseq *seq,
        srfmt fmt,
        srfmtstorage fmt_storage,
        srscheme *scheme,
        srinjection *i,
        srcrcf crc,
        void *compression)
{
	r->e           = e;
	r->a           = a;
	r->seq         = seq;
	r->scheme      = scheme;
	r->fmt         = fmt;
	r->fmt_storage = fmt_storage;
	r->i           = i;
	r->compression = compression;
	r->crc         = crc;
}

#endif
