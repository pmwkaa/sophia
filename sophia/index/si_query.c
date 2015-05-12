
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
                 void *prefix, uint32_t prefixsize,
                 void *key, uint32_t keysize)
{
	q->order   = o;
	q->key     = key;
	q->keysize = keysize;
	q->vlsn    = vlsn;
	q->index   = i;
	q->r       = r;
	q->cache   = c;
	q->prefix  = prefix;
	q->prefixsize = prefixsize;
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
si_querydup(siquery *q, sv *result)
{
	svv *v = sv_vdup(q->r->a, result);
	if (srunlikely(v == NULL)) {
		sr_error(q->r->e, "%s", "memory allocation failed");
		return -1;
	}
	sv_init(&q->result, &sv_vif, v, NULL);
	return 1;
}

static inline int
si_qresult(siquery *q, sriter *i)
{
	sv *v = sr_iteratorof(i);
	if (srunlikely(v == NULL))
		return 0;
	if (srunlikely(sv_flags(v) & SVDELETE))
		return 2;
	int rc = 1;
	if (q->prefix) {
		rc = sr_compareprefix(q->r->cmp, q->prefix, q->prefixsize,
		                      sv_pointer(v),
		                      sv_size(v));
	}
	if (srunlikely(si_querydup(q, v) == -1))
		return -1;
	return rc;
}

static inline int
si_qmatchindex(siquery *q, sinode *node)
{
	svindex *second;
	svindex *first = si_nodeindex_priority(node, &second);
	sriter i;
	sr_iterinit(sv_indexiter, &i, q->r);
	int rc;
	rc = sr_iteropen(sv_indexiter, &i, first, q->order,
	                 q->key, q->keysize, q->vlsn);
	if (rc) {
		return si_qresult(q, &i);
	}
	if (srlikely(second == NULL))
		return 0;
	rc = sr_iteropen(sv_indexiter, &i, second, q->order,
	                 q->key, q->keysize, q->vlsn);
	if (rc) {
		return si_qresult(q, &i);
	}
	return 0;
}

static inline sdpage*
si_qread(srbuf *buf, srbuf *bufxf, sr *r, si *i, sinode *n, sibranch *b,
         sdindexpage *ref)
{
	sr_bufreset(bufxf);
	int rc = sr_bufensure(bufxf, r->a, b->index.h->sizevmax);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return NULL;
	}
	sr_bufreset(buf);
	rc = sr_bufensure(buf, r->a, sizeof(sdpage) + ref->sizeorigin);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return NULL;
	}
	sr_bufadvance(buf, sizeof(sdpage));

	uint64_t offset =
		b->index.h->offset + sd_indexsize(b->index.h) +
		ref->offset;
	if (i->conf->compression)
	{
		/* read compressed page */
		sr_bufreset(&i->readbuf);
		rc = sr_bufensure(&i->readbuf, r->a, ref->size);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "%s", "memory allocation failed");
			return NULL;
		}
		rc = sr_filepread(&n->file, offset, i->readbuf.s, ref->size);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' read error: %s",
			         n->file.file, strerror(errno));
			return NULL;
		}
		sr_bufadvance(&i->readbuf, ref->size);

		/* copy header */
		memcpy(buf->p, i->readbuf.s, sizeof(sdpageheader));
		sr_bufadvance(buf, sizeof(sdpageheader));

		/* decompression */
		srfilter f;
		rc = sr_filterinit(&f, (srfilterif*)r->compression, r, SR_FOUTPUT);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' decompression error", n->file.file);
			return NULL;
		}
		int size = ref->size - sizeof(sdpageheader);
		rc = sr_filternext(&f, buf, i->readbuf.s + sizeof(sdpageheader), size);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' decompression error", n->file.file);
			return NULL;
		}
		sr_filterfree(&f);
	} else {
		rc = sr_filepread(&n->file, offset, buf->s + sizeof(sdpage), ref->sizeorigin);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' read error: %s",
			         n->file.file, strerror(errno));
			return NULL;
		}
		sr_bufadvance(buf, ref->sizeorigin);
	}

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
	sr_iterinit(sd_indexiter, &i, q->r);
	sr_iteropen(sd_indexiter, &i, &b->index, SR_LTE, q->key, q->keysize);
	cb->ref = sr_iterof(sd_indexiter, &i);
	if (cb->ref == NULL)
		return 0;
	sdpage *page = si_qread(&cb->buf_a, &cb->buf_b, q->r, q->index, n, b, cb->ref);
	if (srunlikely(page == NULL)) {
		cb->ref = NULL;
		return -1;
	}
	sr_iterinit(sd_pageiter, &cb->i, q->r);
	int rc;
	rc = sr_iteropen(sd_pageiter, &cb->i, &cb->buf_b, page, q->order,
	                 q->key, q->keysize, q->vlsn);
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
	sr_iterinit(si_iter, &i, q->r);
	sr_iteropen(si_iter, &i, q->index, SR_ROUTE, q->key, q->keysize);
	sinode *node;
	node = sr_iterof(si_iter, &i);
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

static inline void
si_qfetchbranch(siquery *q, sinode *n, sibranch *b, svmerge *m)
{
	sicachebranch *cb = si_cachefollow(q->cache);
	assert(cb->branch == b);
	/* cache iteration */
	if (srlikely(cb->ref)) {
		if (sr_iterhas(sd_pageiter, &cb->i)) {
			svmergesrc *s = sv_mergeadd(m, &cb->i);
			s->ptr = cb;
			q->index->read_cache++;
			return;
		}
	}
	/* read page to cache buffer */
	sriter i;
	sr_iterinit(sd_indexiter, &i, q->r);
	sr_iteropen(sd_indexiter, &i, &b->index, q->order, q->key, q->keysize);
	sdindexpage *prev = cb->ref;
	cb->ref = sr_iterof(sd_indexiter, &i);
	if (cb->ref == NULL || cb->ref == prev)
		return;
	sdpage *page = si_qread(&cb->buf_a, &cb->buf_b, q->r, q->index, n, b, cb->ref);
	if (srunlikely(page == NULL)) {
		cb->ref = NULL;
		return;
	}
	svmergesrc *s = sv_mergeadd(m, &cb->i);
	s->ptr = cb;
	sr_iterinit(sd_pageiter, &cb->i, q->r);
	sr_iteropen(sd_pageiter, &cb->i, &cb->buf_b, page, q->order,
	            q->key, q->keysize, q->vlsn);
}

static inline int
si_qfetch(siquery *q)
{
	sriter i;
	sr_iterinit(si_iter, &i, q->r);
	sr_iteropen(si_iter, &i, q->index, q->order, q->key, q->keysize);
	sinode *node;
next_node:
	node = sr_iterof(si_iter, &i);
	if (srunlikely(node == NULL))
		return 0;

	/* prepare sources */
	svmerge *m = &q->merge;
	int count = node->branch_count + 2;
	int rc = sv_mergeprepare(m, q->r, count);
	if (srunlikely(rc == -1)) {
		sr_errorreset(q->r->e);
		return -1;
	}

	/* in-memory indexes */
	svindex *second;
	svindex *first = si_nodeindex_priority(node, &second);
	svmergesrc *s;
	s = sv_mergeadd(m, NULL);
	sr_iterinit(sv_indexiter, &s->src,q->r);
	sr_iteropen(sv_indexiter, &s->src, first, q->order, q->key, q->keysize, q->vlsn);
	if (srunlikely(second)) {
		s = sv_mergeadd(m, NULL);
		sr_iterinit(sv_indexiter, &s->src, q->r);
		sr_iteropen(sv_indexiter, &s->src, second, q->order, q->key, q->keysize, q->vlsn);
	}

	/* cache and branches */
	rc = si_cachevalidate(q->cache, node);
	if (srunlikely(rc == -1)) {
		sr_error(q->r->e, "%s", "memory allocation failed");
		return -1;
	}
	sibranch *b = node->branch;
	while (b) {
		si_qfetchbranch(q, node, b, m);
		b = b->next;
	}

	/* merge and filter data stream */
	sriter j;
	sr_iterinit(sv_mergeiter, &j, q->r);
	sr_iteropen(sv_mergeiter, &j, m, q->order);
	sriter k;
	sr_iterinit(sv_readiter, &k, q->r);
	sr_iteropen(sv_readiter, &k, &j, q->vlsn);
	sv *v = sr_iterof(sv_readiter, &k);
	if (srunlikely(v == NULL)) {
		sv_mergereset(&q->merge);
		sr_iternext(si_iter, &i);
		goto next_node;
	}

	/* do prefix search */
	rc = 1;
	if (q->prefix) {
		rc = sr_compareprefix(q->r->cmp, q->prefix, q->prefixsize,
		                      sv_pointer(v),
		                      sv_size(v));
	}
	if (srlikely(rc == 1)) {
		if (srunlikely(si_querydup(q, v) == -1))
			return -1;
	}

	/* skip a possible duplicates from data sources */
	sr_iternext(sv_readiter, &k);
	return rc;
}

int si_query(siquery *q)
{
	switch (q->order) {
	case SR_EQ:
	case SR_UPDATE:
		return si_qmatch(q);
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
	sr_iterinit(sd_indexiter, &i, r);
	sr_iteropen(sd_indexiter, &i, &b->index, SR_LTE, sv_pointer(v), sv_size(v));
	sdindexpage *page = sr_iterof(sd_indexiter, &i);
	if (page == NULL)
		return 0;
	return page->lsnmax >= sv_lsn(v);
}

int si_querycommited(si *index, sr *r, sv *v)
{
	sriter i;
	sr_iterinit(si_iter, &i, r);
	sr_iteropen(si_iter, &i, index, SR_ROUTE, sv_pointer(v), sv_size(v));
	sinode *node;
	node = sr_iterof(si_iter, &i);
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
