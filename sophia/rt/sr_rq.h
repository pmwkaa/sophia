#ifndef SR_RQ_H_
#define SR_RQ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

/* range queue */

typedef struct srrqnode srrqnode;
typedef struct srrqq srrqq;
typedef struct srrq srrq;

struct srrqnode {
	uint32_t q, v;
	srlist link;
};

struct srrqq {
	uint32_t count;
	uint32_t q;
	srlist list;
};

struct srrq {
	uint32_t range_count;
	uint32_t range;
	uint32_t last;
	srrqq *q;
};

static inline void
sr_rqinitnode(srrqnode *n) {
	sr_listinit(&n->link);
	n->q = UINT32_MAX;
	n->v = 0;
}

static inline int
sr_rqinit(srrq *q, sra *a, uint32_t range, uint32_t count)
{
	q->range_count = count + 1 /* zero */;
	q->range = range;
	q->q = sr_malloc(a, sizeof(srrqq) * q->range_count);
	if (srunlikely(q->q == NULL))
		return -1;
	uint32_t i = 0;
	while (i < q->range_count) {
		srrqq *p = &q->q[i];
		sr_listinit(&p->list);
		p->count = 0;
		p->q = i;
		i++;
	}
	q->last = 0;
	return 0;
}

static inline void
sr_rqfree(srrq *q, sra *a) {
	sr_free(a, q->q);
}

static inline void
sr_rqadd(srrq *q, srrqnode *n, uint32_t v)
{
	uint32_t pos;
	if (srunlikely(v == 0)) {
		pos = 0;
	} else {
		pos = (v / q->range) + 1;
		if (srunlikely(pos >= q->range_count))
			pos = q->range_count - 1;
	}
	srrqq *p = &q->q[pos];
	sr_listinit(&n->link);
	n->v = v;
	n->q = pos;
	sr_listappend(&p->list, &n->link);
	if (srunlikely(p->count == 0)) {
		if (pos > q->last)
			q->last = pos;
	}
	p->count++;
}

static inline void
sr_rqdelete(srrq *q, srrqnode *n)
{
	srrqq *p = &q->q[n->q];
	p->count--;
	sr_listunlink(&n->link);
	if (srunlikely(p->count == 0 && q->last == n->q))
	{
		int i = n->q - 1;
		while (i >= 0) {
			srrqq *p = &q->q[i];
			if (p->count > 0) {
				q->last = i;
				return;
			}
			i--;
		}
	}
}

static inline void
sr_rqupdate(srrq *q, srrqnode *n, uint32_t v)
{
	if (srlikely(n->q != UINT32_MAX))
		sr_rqdelete(q, n);
	sr_rqadd(q, n, v);
}

static inline srrqnode*
sr_rqprev(srrq *q, srrqnode *n)
{
	int pos;
	srrqq *p;
	if (srlikely(n)) {
		pos = n->q;
		p = &q->q[pos];
		if (n->link.next != (&p->list)) {
			return srcast(n->link.next, srrqnode, link);
		}
		pos--;
	} else {
		pos = q->last;
	}
	for (; pos >= 0; pos--) {
		p = &q->q[pos];
		if (srunlikely(p->count == 0))
			continue;
		return srcast(p->list.next, srrqnode, link);
	}
	return NULL;
}

#endif
