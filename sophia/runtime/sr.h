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
	sfupsert *fmt_upsert;
	sfstorage fmt_storage;
	sf fmt;
	srscheme *scheme;
	srseq *seq;
	ssa *a;
	ssvfs *vfs;
	ssquota *quota;
	ssinjection *i;
	srstat *stat;
	sscrcf crc;
};

static inline void
sr_init(sr *r,
        srerror *e,
        ssa *a,
        ssvfs *vfs,
        ssquota *quota,
        srseq *seq,
        sf fmt,
        sfstorage fmt_storage,
        sfupsert *fmt_upsert,
        srscheme *scheme,
        ssinjection *i,
		srstat *stat,
        sscrcf crc)
{
	r->e           = e;
	r->a           = a;
	r->vfs         = vfs;
	r->quota       = quota;
	r->seq         = seq;
	r->scheme      = scheme;
	r->fmt         = fmt;
	r->fmt_storage = fmt_storage;
	r->fmt_upsert  = fmt_upsert;
	r->i           = i;
	r->stat        = stat;
	r->crc         = crc;
}

#endif
