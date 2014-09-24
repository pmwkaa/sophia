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
};

static inline void
sr_init(sr *r, sra *a, srseq *seq, srcomparator *cmp)
{
	r->cmp = cmp;
	r->seq = seq;
	r->a   = a;
}

#endif
