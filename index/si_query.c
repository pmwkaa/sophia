
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

int si_queryopen(siquery *q, sr *r, si *i, srorder o,
                 uint64_t lsvn,
                 void *key, uint32_t keysize)
{
	q->order    = o;
	q->key      = key;
	q->keysize  = keysize;
	q->lsvn     = lsvn;
	q->index    = i;
	q->r        = r;
	q->firstsrc = NULL;
	memset(&q->result, 0, sizeof(q->result));
	sv_mergeinit(&q->merge);
	si_lock(q->index);
	return 0;
}

int si_queryclose(siquery *q)
{
	si_unlock(q->index);
	sv_mergefree(&q->merge, q->r->a);
	return 0;
}

static inline int
si_qresult(siquery *q, sriter *i)
{
	sv *v = sr_iterof(i);
	if (srunlikely(v == NULL))
		return 0;
	if (srunlikely(svflags(v) & SVDELETE))
		return 2;
	q->result = *v;
	return 1;
}

static inline int
si_qmatchindex(siquery *q, sinode *node)
{
	sriter i;
	sr_iterinit(&i, &sv_indexiter, q->r);
	int rc = sr_iteropen(&i, &node->i0, q->order, q->key, q->keysize, q->lsvn);
	if (rc)
		return si_qresult(q, &i);
	if (! (node->flags & SI_I1))
		return 0;
	sr_iterinit(&i, &sv_indexiter, q->r);
	rc = sr_iteropen(&i, &node->i1, q->order, q->key, q->keysize, q->lsvn);
	if (rc)
		return si_qresult(q, &i);
	return 0;
}

static inline int
si_qmatchnode(siquery *q, sinode *n)
{
	sriter i;
	sr_iterinit(&i, &sd_indexiter, q->r);
	sr_iteropen(&i, &n->index, SR_LTE, q->key, q->keysize);
	sdindexpage *pageref = sr_iterof(&i);
	if (pageref == NULL)
		return 0;
	sdpageheader *h =
		(sdpageheader*)((char*)n->map.p + pageref->offset);
	sdpage page;
	sd_pageinit(&page, h);
	sr_iterinit(&i, &sd_pageiter, q->r);
	int rc = sr_iteropen(&i, &page, q->order, q->key, q->keysize, q->lsvn);
	if (rc == 0)
		return 0;
	return si_qresult(q, &i);
}

static inline int
si_qmatch(siquery *q)
{
	sriter i;
	sr_iterinit(&i, &si_iter, q->r);
	sr_iteropen(&i, q->index, SR_ROUTE, q->key, q->keysize);
	sinode *node;
	node = sr_iterof(&i);
	assert(node != NULL);
	/* search in memory */
	int rc;
	rc = si_qmatchindex(q, node);
	switch (rc) {
	case   2: rc = 0; /* delete */
	case  -1: /* error */
	case   1: return rc;
	}
	/* search on disk */
	sinode *n = node->next;
	while (n) {
		assert(n->icount == 0);
		rc = si_qmatchnode(q, n);
		switch (rc) {
		case  2: rc = 0;
		case -1: 
		case  1: return rc;
		}
		n = n->next;
	}
	n = node;
	rc = si_qmatchnode(q, n);
	if (rc == 2)
		rc = 0;
	return rc;
}

int si_querydup(siquery *q, sv *result)
{
	svv *v = sv_valloc(q->r->a, &q->result);
	if (srunlikely(v == NULL))
		return -1;
	svinit(result, &sv_vif, v, NULL);
	return 1;
}

static inline void
si_qfetchnode(siquery *q, sinode *n, svmerge *m)
{
	sriter i;
	sr_iterinit(&i, &sd_indexiter, q->r);
	sr_iteropen(&i, &n->index, q->order, q->key, q->keysize);
	sdindexpage *pageref = sr_iterof(&i);
	if (pageref == NULL)
		return;
	svmergesrc *s = sv_mergeadd(m);
	sdpage *page = (sdpage*)((char*)s + sizeof(svmergesrc));
	sdpageheader *h =
		(sdpageheader*)((char*)n->map.p + pageref->offset);
	sd_pageinit(page, h);
	sr_iterinit(&s->i, &sd_pageiter, q->r);
	sr_iteropen(&s->i, page, q->order, q->key, q->keysize, q->lsvn);
}

static inline int
si_qfetch(siquery *q)
{
	sriter i;
	sr_iterinit(&i, &si_iter, q->r);
	sr_iteropen(&i, q->index, q->order, q->key, q->keysize);
	sinode *node;
	node = sr_iterof(&i);
	assert(node != NULL);

	/* prepare sources */
	svmerge *m = &q->merge;
	int count = 1 + 2 + node->lv + 1;
	int rc = sv_mergeprepare(m, q->r->a, count, sizeof(sdpage));
	if (srunlikely(rc == -1))
		return -1;
	svmergesrc *s;
	s = sv_mergeadd(m);
	assert(q->firstsrc != NULL);
	s->i = *q->firstsrc;
	s = sv_mergeadd(m);
	sr_iterinit(&s->i, &sv_indexiter, q->r);
	sr_iteropen(&s->i, &node->i1, q->order, q->key, q->keysize, q->lsvn);
	s = sv_mergeadd(m);
	sr_iterinit(&s->i, &sv_indexiter, q->r);
	sr_iteropen(&s->i, &node->i0, q->order, q->key, q->keysize, q->lsvn);
	sinode *n = node->next;
	while (n) {
		si_qfetchnode(q, n, m);
		n = n->next;
	}
	si_qfetchnode(q, node, m);
	/* peek min/max */
	sr_iterinit(&i, &sv_mergeiter, q->r);
	sr_iteropen(&i, m, q->order);
	sriter j;
	sr_iterinit(&j, &sv_seaveiter, q->r);
	sr_iteropen(&j, &i, UINT64_MAX, 0, q->lsvn);
	rc = si_qresult(q, &j);
	return rc;
}

int si_queryfirstsrc(siquery *q, sriter *i)
{
	q->firstsrc = i;
	return 0;
}

int si_query(siquery *q)
{
	switch (q->order) {
	case SR_LT:
	case SR_LTE:
	case SR_GT:
	case SR_GTE:
		return si_qfetch(q);
	case SR_EQ:
	case SR_UPDATE:
		return si_qmatch(q);
	default:
		break;
	}
	return -1;
}

static int
si_querycommited_node(sr *r, sinode *n, sv *v)
{
	sriter i;
	sr_iterinit(&i, &sd_indexiter, r);
	sr_iteropen(&i, &n->index, SR_LTE, svkey(v), svkeysize(v));
	sdindexpage *page = sr_iterof(&i);
	if (page == NULL)
		return 0;
	return page->lsnmax >= svlsn(v);
}

int si_querycommited(si *index, sr *r, sv *v)
{
	sriter i;
	sr_iterinit(&i, &si_iter, r);
	sr_iteropen(&i, index, SR_ROUTE, svkey(v), svkeysize(v));
	sinode *node;
	node = sr_iterof(&i);
	assert(node != NULL);
	sinode *n = node->next;
	int rc;
	while (n) {
		rc = si_querycommited_node(r, n, v);
		if (rc)
			return 1;
		n = n->next;
	}
	n = node;
	rc = si_querycommited_node(r, n, v);
	return rc;
}
