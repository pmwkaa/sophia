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
	sfupdate *fmt_update;
	sfstorage fmt_storage;
	sf fmt;
	srscheme *scheme;
	srseq *seq;
	ssa *a;
	ssquota *quota;
	ssinjection *i;
	void *compression;
	sscrcf crc;
};

static inline void
sr_init(sr *r,
        srerror *e,
        ssa *a,
        ssquota *quota,
        srseq *seq,
        sf fmt,
        sfstorage fmt_storage,
        sfupdate *fmt_update,
        srscheme *scheme,
        ssinjection *i,
        sscrcf crc,
        void *compression)
{
	r->e           = e;
	r->a           = a;
	r->quota       = quota;
	r->seq         = seq;
	r->scheme      = scheme;
	r->fmt         = fmt;
	r->fmt_storage = fmt_storage;
	r->fmt_update  = fmt_update;
	r->i           = i;
	r->compression = compression;
	r->crc         = crc;
}

#endif
