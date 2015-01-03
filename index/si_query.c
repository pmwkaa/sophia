
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

int si_queryopen(siquery *q, sr *r, sicache *c, si *i, srorder o,
                 uint64_t vlsn,
                 void *key, uint32_t keysize)
{
	q->order   = o;
	q->key     = key;
	q->keysize = keysize;
	q->vlsn    = vlsn;
	q->index   = i;
	q->r       = r;
	q->cache   = c;
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
	int rc = sr_iteropen(&i, &node->i0, q->order, q->key, q->keysize, q->vlsn);
	if (rc)
		return si_qresult(q, &i);
	if (! (node->flags & SI_I1))
		return 0;
	sr_iterinit(&i, &sv_indexiter, q->r);
	rc = sr_iteropen(&i, &node->i1, q->order, q->key, q->keysize, q->vlsn);
	if (rc)
		return si_qresult(q, &i);
	return 0;
}

static inline sdpage*
si_qread(srbuf *buf, sr *r, si *i, sinode *n,
         sibranch *b, sdindexpage *ref)
{
	int size = sizeof(sdpage) + ref->size;
	int rc = sr_bufensure(buf, r->a, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		sr_error_recoverable(r->e);
		return NULL;
	}
	uint64_t offset =
		b->index.h->offset + sd_indexsize(b->index.h) +
	    ref->offset;
	rc = sr_filepread(&n->file, offset, buf->s + sizeof(sdpage), ref->size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
		         n->file.file, strerror(errno));
		sr_error_recoverable(r->e);
		return NULL;
	}
	sr_bufadvance(buf, size);
	i->read_disk++;
	sdpageheader *h = (sdpageheader*)(buf->s + sizeof(sdpage));
	sdpage *page = (sdpage*)(buf->s);
	sd_pageinit(page, h);
	return page;
}

static inline int
si_qmatchbranch(siquery *q, sinode *n, sibranch *b)
{
	sicachebranch *cb = si_cachefollow(q->cache);
	assert(cb->branch == b);
	sriter i;
	sr_iterinit(&i, &sd_indexiter, q->r);
	sr_iteropen(&i, &b->index, SR_LTE, q->key, q->keysize);
	cb->ref = sr_iterof(&i);
	if (cb->ref == NULL)
		return 0;
	sdpage *page = si_qread(&cb->buf, q->r, q->index, n, b, cb->ref);
	if (srunlikely(page == NULL)) {
		cb->ref = NULL;
		return -1;
	}
	sr_iterinit(&cb->i, &sd_pageiter, q->r);
	int rc;
	rc = sr_iteropen(&cb->i, page, q->order, q->key, q->keysize, q->vlsn);
	if (rc == 0) {
		cb->ref = NULL;
		return 0;
	}
	return si_qresult(q, &cb->i);
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
	case  2: rc = 0; /* delete */
	case -1: /* error */
	case  1: return rc;
	}
	/* */
	rc = si_cachevalidate(q->cache, node);
	if (srunlikely(rc == -1)) {
		sr_error(q->r->e, "%s", "memory allocation failed");
		sr_error_recoverable(q->r->e);
		return -1;
	}
	/* search on disk */
	sibranch *b = node->branch;
	while (b) {
		rc = si_qmatchbranch(q, node, b);
		switch (rc) {
		case  2: rc = 0;
		case -1: 
		case  1: return rc;
		}
		b = b->next;
	}
	return 0;
}

int si_querydup(siquery *q, sv *result)
{
	svv *v = sv_valloc(q->r->a, &q->result);
	if (srunlikely(v == NULL)) {
		sr_error(q->r->e, "%s", "memory allocation failed");
		sr_error_recoverable(q->r->e);
		return -1;
	}
	svinit(result, &sv_vif, v, NULL);
	return 1;
}

static inline void
si_qfetchbranch(siquery *q, sinode *n, sibranch *b, svmerge *m)
{
	sicachebranch *cb = si_cachefollow(q->cache);
	assert(cb->branch == b);
	/* cache iteration */
	if (srlikely(cb->ref)) {
		if (sr_iterhas(&cb->i)) {
			svmergesrc *s = sv_mergeadd(m, &cb->i);
			s->ptr = cb;
			q->index->read_cache++;
			return;
		}
	}
	/* read page to cache buffer */
	sriter i;
	sr_iterinit(&i, &sd_indexiter, q->r);
	sr_iteropen(&i, &b->index, q->order, q->key, q->keysize);
	sdindexpage *prev = cb->ref;
	cb->ref = sr_iterof(&i);
	if (cb->ref == NULL || cb->ref == prev)
		return;
	sdpage *page = si_qread(&cb->buf, q->r, q->index, n, b, cb->ref);
	if (srunlikely(page == NULL)) {
		cb->ref = NULL;
		return;
	}
	svmergesrc *s = sv_mergeadd(m, &cb->i);
	s->ptr = cb;
	sr_iterinit(&cb->i, &sd_pageiter, q->r);
	sr_iteropen(&cb->i, page, q->order, q->key, q->keysize, q->vlsn);
}

static inline int
si_qfetch(siquery *q)
{
	sriter i;
	sr_iterinit(&i, &si_iter, q->r);
	sr_iteropen(&i, q->index, q->order, q->key, q->keysize);
	sinode *node;
next_node:
	node = sr_iterof(&i);
	if (srunlikely(node == NULL))
		return 0;

	/* prepare sources */
	svmerge *m = &q->merge;
	int count = node->branch_count + 2;
	int rc = sv_mergeprepare(m, q->r, count);
	if (srunlikely(rc == -1)) {
		sr_error_recoverable(q->r->e);
		return -1;
	}
	svmergesrc *s;
	s = sv_mergeadd(m, NULL);
	sr_iterinit(&s->src, &sv_indexiter, q->r);
	sr_iteropen(&s->src, &node->i1, q->order, q->key, q->keysize, q->vlsn);
	s = sv_mergeadd(m, NULL);
	sr_iterinit(&s->src, &sv_indexiter, q->r);
	sr_iteropen(&s->src, &node->i0, q->order, q->key, q->keysize, q->vlsn);

	/* */
	rc = si_cachevalidate(q->cache, node);
	if (srunlikely(rc == -1)) {
		sr_error(q->r->e, "%s", "memory allocation failed");
		sr_error_recoverable(q->r->e);
		return -1;
	}
	sibranch *b = node->branch;
	while (b) {
		si_qfetchbranch(q, node, b, m);
		b = b->next;
	}

	/* merge and filter data stream */
	sriter j;
	sr_iterinit(&j, &sv_mergeiter, q->r);
	sr_iteropen(&j, m, q->order);
	sriter k;
	sr_iterinit(&k, &sv_readiter, q->r);
	sr_iteropen(&k, &j, q->vlsn);
	sv *v = sr_iterof(&k);
	if (srunlikely(v == NULL)) {
		sv_mergereset(&q->merge);
		sr_iternext(&i);
		goto next_node;
	}
	q->result = *v;

	/* skip a possible duplicates from data sources */
	sr_iternext(&k);
	return 1;
}

int si_query(siquery *q)
{
	switch (q->order) {
	case SR_EQ:
	case SR_UPDATE:
		return si_qmatch(q);
	case SR_RANDOM:
	case SR_LT:
	case SR_LTE:
	case SR_GT:
	case SR_GTE:
		return si_qfetch(q);
	default:
		break;
	}
	return -1;
}

static int
si_querycommited_branch(sr *r, sibranch *b, sv *v)
{
	sriter i;
	sr_iterinit(&i, &sd_indexiter, r);
	sr_iteropen(&i, &b->index, SR_LTE, svkey(v), svkeysize(v));
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
	sibranch *b = node->branch;
	int rc;
	while (b) {
		rc = si_querycommited_branch(r, b, v);
		if (rc)
			return 1;
		b = b->next;
	}
	rc = si_querycommited_branch(r, &node->self, v);
	return rc;
}
