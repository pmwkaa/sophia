
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
	q->update_v   = NULL;
	q->update_eq  = 0;
	q->cache_only = 0;
	memset(&q->result, 0, sizeof(q->result));
	sv_mergeinit(&q->merge);
	si_lock(q->index);
	return 0;
}

void si_querycache_only(siquery *q)
{
	q->cache_only = 1;
}

void si_queryupdate(siquery *q, sv *v, int eq)
{
	q->update_v  = v;
	q->update_eq = eq;
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
si_qgetresult(siquery *q, sv *v, int compare)
{
	int rc;
	if (compare) {
		rc = sr_compare(q->r->scheme, sv_pointer(v), sv_size(v),
		                q->key, q->keysize);
		if (ssunlikely(rc != 0))
			return 0;
	}
	if (q->prefix) {
		rc = sr_compareprefix(q->r->scheme,
		                      q->prefix,
		                      q->prefixsize,
		                      sv_pointer(v), sv_size(v));
		if (ssunlikely(! rc))
			return 0;
	}
	if (ssunlikely(sv_is(v, SVDELETE)))
		return 2;
	rc = si_querydup(q, v);
	if (ssunlikely(rc == -1))
		return -1;
	return 1;
}

static inline int
si_qgetindex(siquery *q, sinode *node)
{
	svindex *second;
	svindex *first = si_nodeindex_priority(node, &second);
	ssiter i;
	ss_iterinit(sv_indexiter, &i);
	int rc;
	if (first->count > 0) {
		rc = ss_iteropen(sv_indexiter, &i, q->r, first,
		                 SS_GTE, q->key, q->keysize);
		if (rc) {
			goto result;
		}
	}
	if (sslikely(second == NULL || !second->count))
		return 0;
	rc = ss_iteropen(sv_indexiter, &i, q->r, second,
	                 SS_GTE, q->key, q->keysize);
	if (! rc) {
		return 0;
	}
result:;
	sv *v = ss_iterof(sv_indexiter, &i);
	assert(v != NULL);
	svv *visible = sv_visible((svv*)v->v, q->vlsn);
	if (visible == NULL)
		return 0;
	sv vret;
	sv_init(&vret, &sv_vif, visible, NULL);
	return si_qgetresult(q, &vret, 0);
}

static inline int
si_qgetbranch(siquery *q, sinode *n, sibranch *b)
{
	sicachebranch *c = si_cachefollow(q->cache);
	assert(c->branch == b);
	sdreadarg arg = {
		.index           = &b->index,
		.buf             = &c->buf_a,
		.buf_xf          = &c->buf_b,
		.buf_read        = &q->index->readbuf,
		.index_iter      = &c->index_iter,
		.page_iter       = &c->page_iter,
		.use_compression = q->index->scheme->compression,
		.use_memory      = q->index->scheme->in_memory,
		.use_mmap        = q->index->scheme->mmap,
		.use_mmap_copy   = 0,
		.o               = SS_GTE,
		.mmap            = &n->map,
		.memory          = &b->copy,
		.file            = &n->file,
		.r               = q->r
	};
	ss_iterinit(sd_read, &c->i);
	int rc = ss_iteropen(sd_read, &c->i, &arg, q->key, q->keysize);
	q->index->read_disk += sd_read_stat(&c->i);
	if (ssunlikely(rc <= 0))
		return rc;
	/* prepare sources */
	sv_mergereset(&q->merge);
	sv_mergeadd(&q->merge, &c->i);
	ssiter i;
	ss_iterinit(sv_mergeiter, &i);
	ss_iteropen(sv_mergeiter, &i, q->r, &q->merge, SS_GTE);
	ssiter j;
	ss_iterinit(sv_readiter, &j);
	ss_iteropen(sv_readiter, &j, q->r, &i, &q->index->u, q->vlsn, 1);
	sv *v = ss_iterof(sv_readiter, &j);
	if (ssunlikely(v == NULL))
		return 0;
	return si_qgetresult(q, v, 1);
}

static inline int
si_qget(siquery *q)
{
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, q->r, q->index, SS_GTE, q->key, q->keysize);
	sinode *node;
	node = ss_iterof(si_iter, &i);
	assert(node != NULL);
	/* search in memory */
	int rc;
	rc = si_qgetindex(q, node);
	if (rc != 0)
		return rc;
	if (q->cache_only)
		return 2;
	/* */
	rc = si_cachevalidate(q->cache, node);
	if (ssunlikely(rc == -1)) {
		sr_oom(q->r->e);
		return -1;
	}
	svmerge *m = &q->merge;
	rc = sv_mergeprepare(m, q->r, 1);
	assert(rc == 0);
	/* search on disk */
	sibranch *b = node->branch;
	while (b) {
		rc = si_qgetbranch(q, node, b);
		if (rc != 0)
			return rc;
		b = b->next;
	}
	return 0;
}

static inline int
si_qrangebranch(siquery *q, sinode *n, sibranch *b, svmerge *m)
{
	sicachebranch *c = si_cachefollow(q->cache);
	assert(c->branch == b);
	/* iterate cache */
	if (ss_iterhas(sd_read, &c->i)) {
		svmergesrc *s = sv_mergeadd(m, &c->i);
		q->index->read_cache++;
		s->ptr = c;
		return 1;
	}
	if (c->open) {
		return 1;
	}
	if (q->cache_only) {
		return 2;
	}
	c->open = 1;
	sdreadarg arg = {
		.index           = &b->index,
		.buf             = &c->buf_a,
		.buf_xf          = &c->buf_b,
		.buf_read        = &q->index->readbuf,
		.index_iter      = &c->index_iter,
		.page_iter       = &c->page_iter,
		.use_compression = q->index->scheme->compression,
		.use_memory      = q->index->scheme->in_memory,
		.use_mmap        = q->index->scheme->mmap,
		.use_mmap_copy   = 1,
		.o               = q->order,
		.memory          = &b->copy,
		.mmap            = &n->map,
		.file            = &n->file,
		.r               = q->r
	};
	ss_iterinit(sd_read, &c->i);
	int rc = ss_iteropen(sd_read, &c->i, &arg, q->key, q->keysize);
	q->index->read_disk += sd_read_stat(&c->i);
	if (ssunlikely(rc == -1))
		return -1;
	if (ssunlikely(! ss_iterhas(sd_read, &c->i)))
		return 0;
	svmergesrc *s = sv_mergeadd(m, &c->i);
	s->ptr = c;
	return 1;
}

static inline int
si_qrange(siquery *q)
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
	if (first->count) {
		s = sv_mergeadd(m, NULL);
		ss_iterinit(sv_indexiter, &s->src);
		ss_iteropen(sv_indexiter, &s->src, q->r, first, q->order,
		            q->key, q->keysize);
	}
	if (ssunlikely(second && second->count)) {
		s = sv_mergeadd(m, NULL);
		ss_iterinit(sv_indexiter, &s->src);
		ss_iteropen(sv_indexiter, &s->src, q->r, second, q->order,
		            q->key, q->keysize);
	}

	/* cache and branches */
	rc = si_cachevalidate(q->cache, node);
	if (ssunlikely(rc == -1)) {
		sr_oom(q->r->e);
		return -1;
	}
	sibranch *b = node->branch;
	while (b) {
		rc = si_qrangebranch(q, node, b, m);
		if (ssunlikely(rc == -1 || rc == 2))
			return rc;
		b = b->next;
	}

	/* merge and filter data stream */
	ssiter j;
	ss_iterinit(sv_mergeiter, &j);
	ss_iteropen(sv_mergeiter, &j, q->r, m, q->order);
	ssiter k;
	ss_iterinit(sv_readiter, &k);
	ss_iteropen(sv_readiter, &k, q->r, &j, &q->index->u, q->vlsn, 0);
	sv *v = ss_iterof(sv_readiter, &k);
	if (ssunlikely(v == NULL)) {
		sv_mergereset(&q->merge);
		ss_iternext(si_iter, &i);
		goto next_node;
	}

	rc = 1;
	/* convert update search to SS_EQ */
	if (q->update_eq) {
		rc = sr_compare(q->r->scheme, sv_pointer(v), sv_size(v),
		                q->key, q->keysize);
		rc = rc == 0;
	}
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
		return si_qget(q);
	case SS_LT:
	case SS_LTE:
	case SS_GT:
	case SS_GTE:
		return si_qrange(q);
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
	ss_iteropen(sd_indexiter, &i, r, &b->index, SS_GTE,
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
	ss_iteropen(si_iter, &i, r, index, SS_GTE,
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
