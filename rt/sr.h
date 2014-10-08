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
	srcomparator *cmp;
	srseq *seq;
	sra *a;
	srinjection *i;
};

static inline void
sr_init(sr *r, sra *a,
        srseq *seq,
        srcomparator *cmp,
        srinjection *i)
{
	r->cmp = cmp;
	r->seq = seq;
	r->a   = a;
	r->i   = i;
}

#endif
