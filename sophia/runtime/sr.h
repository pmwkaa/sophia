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
	srstatus *status;
	srlog *log;
	srerror *e;
	sfupsert *fmt_upsert;
	sfstorage fmt_storage;
	sfscheme *scheme;
	srseq *seq;
	ssa *a;
	ssvfs *vfs;
	srquota *quota;
	ssinjection *i;
	srstat *stat;
	sscrcf crc;
	void *ptr;
};

static inline void
sr_init(sr *r,
        srstatus *status,
        srlog *log,
        srerror *e,
        ssa *a,
        ssvfs *vfs,
        srquota *quota,
        srseq *seq,
        sfstorage fmt_storage,
        sfupsert *fmt_upsert,
        sfscheme *scheme,
        ssinjection *i,
		srstat *stat,
        sscrcf crc,
        void *ptr)
{
	r->status      = status;
	r->log         = log;
	r->e           = e;
	r->a           = a;
	r->vfs         = vfs;
	r->quota       = quota;
	r->seq         = seq;
	r->scheme      = scheme;
	r->fmt_storage = fmt_storage;
	r->fmt_upsert  = fmt_upsert;
	r->i           = i;
	r->stat        = stat;
	r->crc         = crc;
	r->ptr         = ptr;
}

#endif
