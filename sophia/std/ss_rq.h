#ifndef SS_RQ_H_
#define SS_RQ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

/* range queue */

typedef struct ssrqnode ssrqnode;
typedef struct ssrqq ssrqq;
typedef struct ssrq ssrq;

struct ssrqnode {
	uint32_t q, v;
	sslist link;
};

struct ssrqq {
	uint32_t count;
	uint32_t q;
	sslist list;
};

struct ssrq {
	uint32_t range_count;
	uint32_t range;
	uint32_t last;
	ssrqq *q;
};

static inline void
ss_rqinitnode(ssrqnode *n) {
	ss_listinit(&n->link);
	n->q = UINT32_MAX;
	n->v = 0;
}

static inline int
ss_rqinit(ssrq *q, ssa *a, uint32_t range, uint32_t count)
{
	q->range_count = count + 1 /* zero */;
	q->range = range;
	q->q = ss_malloc(a, sizeof(ssrqq) * q->range_count);
	if (ssunlikely(q->q == NULL))
		return -1;
	uint32_t i = 0;
	while (i < q->range_count) {
		ssrqq *p = &q->q[i];
		ss_listinit(&p->list);
		p->count = 0;
		p->q = i;
		i++;
	}
	q->last = 0;
	return 0;
}

static inline void
ss_rqfree(ssrq *q, ssa *a)
{
	if (q->q) {
		ss_free(a, q->q);
		q->q = NULL;
	}
}

static inline void
ss_rqadd(ssrq *q, ssrqnode *n, uint32_t v)
{
	uint32_t pos;
	if (ssunlikely(v == 0)) {
		pos = 0;
	} else {
		pos = (v / q->range) + 1;
		if (ssunlikely(pos >= q->range_count))
			pos = q->range_count - 1;
	}
	ssrqq *p = &q->q[pos];
	ss_listinit(&n->link);
	n->v = v;
	n->q = pos;
	ss_listappend(&p->list, &n->link);
	if (ssunlikely(p->count == 0)) {
		if (pos > q->last)
			q->last = pos;
	}
	p->count++;
}

static inline void
ss_rqdelete(ssrq *q, ssrqnode *n)
{
	ssrqq *p = &q->q[n->q];
	p->count--;
	ss_listunlink(&n->link);
	if (ssunlikely(p->count == 0 && q->last == n->q))
	{
		int i = n->q - 1;
		while (i >= 0) {
			ssrqq *p = &q->q[i];
			if (p->count > 0) {
				q->last = i;
				return;
			}
			i--;
		}
	}
}

static inline void
ss_rqupdate(ssrq *q, ssrqnode *n, uint32_t v)
{
	if (sslikely(n->q != UINT32_MAX))
		ss_rqdelete(q, n);
	ss_rqadd(q, n, v);
}

static inline ssrqnode*
ss_rqprev(ssrq *q, ssrqnode *n)
{
	int pos;
	ssrqq *p;
	if (sslikely(n)) {
		pos = n->q;
		p = &q->q[pos];
		if (n->link.next != (&p->list)) {
			return sscast(n->link.next, ssrqnode, link);
		}
		pos--;
	} else {
		pos = q->last;
	}
	for (; pos >= 0; pos--) {
		p = &q->q[pos];
		if (ssunlikely(p->count == 0))
			continue;
		return sscast(p->list.next, ssrqnode, link);
	}
	return NULL;
}

#endif
