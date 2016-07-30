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
	sfupsert *upsert;
	sfscheme *scheme;
	srseq *seq;
	ssa *a;
	ssa *av;
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
        ssa *av,
        ssvfs *vfs,
        srquota *quota,
        srseq *seq,
        sfupsert *upsert,
        sfscheme *scheme,
        ssinjection *i,
        srstat *stat,
        sscrcf crc,
        void *ptr)
{
	r->status = status;
	r->log    = log;
	r->e      = e;
	r->a      = a;
	r->av     = av;
	r->vfs    = vfs;
	r->quota  = quota;
	r->seq    = seq;
	r->scheme = scheme;
	r->upsert = upsert;
	r->i      = i;
	r->stat   = stat;
	r->crc    = crc;
	r->ptr    = ptr;
}

#endif
