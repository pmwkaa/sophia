#ifndef SI_AMQF_H_
#define SI_AMQF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline int
si_amqfhas_branch(sr *r, sibranch *b, char *key)
{
	sdindexamqf *qh = sd_indexamqf(&b->index);
	ssqf qf;
	ss_qfrecover(&qf, qh->q, qh->r, qh->size, qh->table);
	return ss_qfhas(&qf, sf_hash(r->scheme, key));
}

static inline int
si_amqfhas(sr *r, sinode *node, char *key)
{
	sibranch *b = node->branch;
	while (b) {
		int rc = si_amqfhas_branch(r, b, key);
		if (rc)
			return rc;
		b = b->next;
	}
	return 0;
}

#endif
