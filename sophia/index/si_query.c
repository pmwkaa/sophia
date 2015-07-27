
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

int si_queryopen(siquery *q, sicache *c, si *i, ssorder o,
                 uint64_t vlsn,
                 void *prefix, uint32_t prefixsize,
                 void *key, uint32_t keysize)
{
	q->order      = o;
	q->key        = key;
	q->keysize    = keysize;
	q->vlsn       = vlsn;
	q->index      = i;
	q->r          = i->r;
	q->cache      = c;
	q->prefix     = prefix;
	q->prefixsize = prefixsize;
	q->update     = 0;
	q->update_v   = NULL;
	memset(&q->result, 0, sizeof(q->result));
	sv_mergeinit(&q->merge);
	si_lock(q->index);
	return 0;
}

void si_queryupdate(siquery *q, sv *v)
{
	q->update = 1;
	q->update_v = v;
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
	if (ssunlikely(v == NULL))
		return sr_oom(q->r->e);
	sv_init(&q->result, &sv_vif, v, NULL);
	return 1;
}

static inline int
si_qresult(siquery *q, ssiter *i)
{
	sv *v = ss_iteratorof(i);
	if (ssunlikely(v == NULL))
		return 0;
	if (ssunlikely(sv_is(v, SVDELETE)))
		return 2;
	int rc = 1;
	if (q->prefix) {
		rc = sr_compareprefix(q->r->scheme, q->prefix, q->prefixsize,
		                      sv_pointer(v),
		                      sv_size(v));
	}
	if (ssunlikely(si_querydup(q, v) == -1))
		return -1;
	return rc;
}

static inline int
si_qmatchindex(siquery *q, sinode *node)
{
	svindex *second;
	svindex *first = si_nodeindex_priority(node, &second);
	ssiter i;
	ss_iterinit(sv_indexiter, &i);
	int rc;
	rc = ss_iteropen(sv_indexiter, &i, q->r, first, q->order,
	                 q->key, q->keysize, q->vlsn);
	if (rc) {
		return si_qresult(q, &i);
	}
	if (sslikely(second == NULL))
		return 0;
	rc = ss_iteropen(sv_indexiter, &i, q->r, second, q->order,
	                 q->key, q->keysize, q->vlsn);
	if (rc) {
		return si_qresult(q, &i);
	}
	return 0;
}

static inline int
si_qread(sdpage *page, sr *r, si *i, sinode *n,
         ssbuf *buf, ssbuf *bufxf,
         sibranch *b,
         sdindexpage *ref, int mmap_copy)
{
	i->read_disk++;
	ss_bufreset(bufxf);
	int rc = ss_bufensure(bufxf, r->a, b->index.h->sizevmax);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	ss_bufreset(buf);
	rc = ss_bufensure(buf, r->a, ref->sizeorigin);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);

	uint64_t offset =
		b->index.h->offset + sd_indexsize(b->index.h) +
		ref->offset;

	/* compression */
	if (i->scheme->compression)
	{
		char *page_pointer;
		if (! i->scheme->mmap) {
			ss_bufreset(&i->readbuf);
			rc = ss_bufensure(&i->readbuf, r->a, ref->size);
			if (ssunlikely(rc == -1))
				return sr_oom(r->e);
			rc = ss_filepread(&n->file, offset, i->readbuf.s, ref->size);
			if (ssunlikely(rc == -1)) {
				sr_error(r->e, "db file '%s' read error: %s",
						 n->file.file, strerror(errno));
				return -1;
			}
			ss_bufadvance(&i->readbuf, ref->size);
			page_pointer = i->readbuf.s;
		} else {
			page_pointer = n->map.p + offset;
		}

		/* copy header */
		memcpy(buf->p, page_pointer, sizeof(sdpageheader));
		ss_bufadvance(buf, sizeof(sdpageheader));

		/* decompression */
		ssfilter f;
		rc = ss_filterinit(&f, (ssfilterif*)r->compression, r->a, SS_FOUTPUT);
		if (ssunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' decompression error", n->file.file);
			return -1;
		}
		int size = ref->size - sizeof(sdpageheader);
		rc = ss_filternext(&f, buf, page_pointer + sizeof(sdpageheader), size);
		if (ssunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' decompression error", n->file.file);
			return -1;
		}
		ss_filterfree(&f);
		sd_pageinit(page, (sdpageheader*)buf->s);
		return 0;
	}

	/* mmap */
	if (i->scheme->mmap) {
		if (mmap_copy) {
			memcpy(buf->s, n->map.p + offset, ref->sizeorigin);
			sd_pageinit(page, (sdpageheader*)(buf->s));
		} else {
			sd_pageinit(page, (sdpageheader*)(n->map.p + offset));
		}
		return 0;
	}

	/* default */
	rc = ss_filepread(&n->file, offset, buf->s, ref->sizeorigin);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
				 n->file.file, strerror(errno));
		return -1;
	}
	ss_bufadvance(buf, ref->sizeorigin);
	sd_pageinit(page, (sdpageheader*)(buf->s));
	return 0;
}

static inline int
si_qmatchbranch(siquery *q, sinode *n, sibranch *b)
{
	sicachebranch *cb = si_cachefollow(q->cache);
	assert(cb->branch == b);
	ssiter i;
	ss_iterinit(sd_indexiter, &i);
	ss_iteropen(sd_indexiter, &i, q->r, &b->index, SS_LTE,
	            q->key, q->keysize);
	cb->ref = ss_iterof(sd_indexiter, &i);
	if (cb->ref == NULL)
		return 0;
	int rc;
	rc = si_qread(&cb->page, q->r, q->index, n,
	              &cb->buf_a, &cb->buf_b, b,
	               cb->ref, 0);
	if (ssunlikely(rc == -1)) {
		cb->ref = NULL;
		return -1;
	}
	ss_iterinit(sd_pageiter, &cb->i);
	rc = ss_iteropen(sd_pageiter, &cb->i, q->r, &cb->buf_b,
	                 &cb->page, q->order,
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
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, q->r, q->index, SS_ROUTE, q->key, q->keysize);
	sinode *node;
	node = ss_iterof(si_iter, &i);
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
	if (ssunlikely(rc == -1)) {
		sr_oom(q->r->e);
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
	if (sslikely(cb->ref)) {
		if (ss_iterhas(sd_pageiter, &cb->i)) {
			svmergesrc *s = sv_mergeadd(m, &cb->i);
			s->ptr = cb;
			q->index->read_cache++;
			return;
		}
	}
	/* read page to cache buffer */
	ssiter i;
	ss_iterinit(sd_indexiter, &i);
	ss_iteropen(sd_indexiter, &i, q->r, &b->index, q->order, q->key, q->keysize);
	sdindexpage *prev = cb->ref;
next_page:
	cb->ref = ss_iterof(sd_indexiter, &i);
	if (cb->ref == NULL || cb->ref == prev)
		return;
	int rc = si_qread(&cb->page, q->r, q->index, n,
	                  &cb->buf_a,
	                  &cb->buf_b, b, cb->ref, 1);
	if (ssunlikely(rc == -1)) {
		cb->ref = NULL;
		return;
	}
	ss_iterinit(sd_pageiter, &cb->i);
	ss_iteropen(sd_pageiter, &cb->i, q->r, &cb->buf_b, &cb->page, q->order,
	            q->key, q->keysize, q->vlsn);
	if (ssunlikely(! ss_iterof(sd_pageiter, &cb->i))) {
		ss_iternext(sd_indexiter, &i);
		goto next_page;
	}
	svmergesrc *s = sv_mergeadd(m, &cb->i);
	s->ptr = cb;
}

static inline int
si_qfetch(siquery *q)
{
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, q->r, q->index, q->order, q->key, q->keysize);
	sinode *node;
next_node:
	node = ss_iterof(si_iter, &i);
	if (ssunlikely(node == NULL))
		return 0;

	/* prepare sources */
	svmerge *m = &q->merge;
	int count = node->branch_count + 2 + 1;
	int rc = sv_mergeprepare(m, q->r, count);
	if (ssunlikely(rc == -1)) {
		sr_errorreset(q->r->e);
		return -1;
	}

	/* external source (update) */
	svmergesrc *s;
	sv upbuf_reserve;
	ssbuf upbuf;
	if (ssunlikely(q->update_v && q->update_v->v)) {
		ss_bufinit_reserve(&upbuf, &upbuf_reserve, sizeof(upbuf_reserve));
		ss_bufadd(&upbuf, NULL, (void*)&q->update_v, sizeof(sv*));
		s = sv_mergeadd(m, NULL);
		ss_iterinit(ss_bufiterref, &s->src);
		ss_iteropen(ss_bufiterref, &s->src, &upbuf, sizeof(sv*));
	}

	/* in-memory indexes */
	svindex *second;
	svindex *first = si_nodeindex_priority(node, &second);
	s = sv_mergeadd(m, NULL);
	ss_iterinit(sv_indexiter, &s->src);
	ss_iteropen(sv_indexiter, &s->src, q->r, first, q->order,
	            q->key, q->keysize, q->vlsn);
	if (ssunlikely(second)) {
		s = sv_mergeadd(m, NULL);
		ss_iterinit(sv_indexiter, &s->src);
		ss_iteropen(sv_indexiter, &s->src, q->r, second, q->order,
		            q->key, q->keysize, q->vlsn);
	}

	/* cache and branches */
	rc = si_cachevalidate(q->cache, node);
	if (ssunlikely(rc == -1)) {
		sr_oom(q->r->e);
		return -1;
	}
	sibranch *b = node->branch;
	while (b) {
		si_qfetchbranch(q, node, b, m);
		b = b->next;
	}

	/* merge and filter data stream */
	ssiter j;
	ss_iterinit(sv_mergeiter, &j);
	ss_iteropen(sv_mergeiter, &j, q->r, m, q->order, 0);
	ssiter k;
	ss_iterinit(sv_readiter, &k);
	ss_iteropen(sv_readiter, &k, &j, q->vlsn);
	sv *v = ss_iterof(sv_readiter, &k);
	if (ssunlikely(v == NULL)) {
		sv_mergereset(&q->merge, q->r->a);
		ss_iternext(si_iter, &i);
		goto next_node;
	}

	rc = 1;
#if 0
	/* do update validation */
	if (q->update) {
		rc = sr_compare(q->r->scheme, sv_pointer(v), sv_size(v),
		                q->key, q->keysize);
		rc = rc == 0;
	}
#endif
	/* do prefix search */
	if (q->prefix && rc) {
		rc = sr_compareprefix(q->r->scheme, q->prefix, q->prefixsize,
		                      sv_pointer(v),
		                      sv_size(v));
	}
	if (sslikely(rc == 1)) {
		if (ssunlikely(si_querydup(q, v) == -1))
			return -1;
	}

	/* skip a possible duplicates from data sources */
	ss_iternext(sv_readiter, &k);
	return rc;
}

int si_query(siquery *q)
{
	switch (q->order) {
	case SS_EQ:
	case SS_HAS:
		return si_qmatch(q);
	case SS_LT:
	case SS_LTE:
	case SS_GT:
	case SS_GTE:
		return si_qfetch(q);
	default:
		break;
	}
	return -1;
}

static int
si_querycommited_branch(sr *r, sibranch *b, sv *v)
{
	ssiter i;
	ss_iterinit(sd_indexiter, &i);
	ss_iteropen(sd_indexiter, &i, r, &b->index, SS_LTE,
	            sv_pointer(v), sv_size(v));
	sdindexpage *page = ss_iterof(sd_indexiter, &i);
	if (page == NULL)
		return 0;
	return page->lsnmax >= sv_lsn(v);
}

int si_querycommited(si *index, sr *r, sv *v)
{
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, r, index, SS_ROUTE,
	            sv_pointer(v), sv_size(v));
	sinode *node;
	node = ss_iterof(si_iter, &i);
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
