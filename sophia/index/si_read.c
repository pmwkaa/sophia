
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
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

int si_readopen(siread *q, si *i, sicache *c, ssorder o,
                uint64_t vlsn,
                char *key,
                char *upsert,
                char *prefix, uint32_t prefix_size,
                int has,
                int read_start)
{
	q->order       = o;
	q->key         = key;
	q->vlsn        = vlsn;
	q->index       = i;
	q->r           = &i->r;
	q->cache       = c;
	q->prefix      = prefix;
	q->prefix_size = prefix_size;
	q->has         = has;
	q->read_start  = read_start;
	q->read_disk   = 0;
	q->read_cache  = 0;
	q->upsert      = upsert;
	q->upsert_eq   = 0;
	q->result      = NULL;
	if (!has && sf_upserthas(&i->scheme.upsert)) {
		if (q->order == SS_EQ) {
			q->upsert_eq = 1;
			q->order = SS_GTE;
		}
	}
	sv_mergeinit(&q->merge);
	si_lock(i);
	return 0;
}

int si_readclose(siread *q)
{
	si_unlock(q->index);
	sv_mergefree(&q->merge, q->r->a);
	return 0;
}

static inline int
si_readdup(siread *q, char *result)
{
	q->result = sv_vbuildraw(q->r, result);
	if (ssunlikely(q->result == NULL))
		return sr_oom(q->r->e);
	return 1;
}

static inline void
si_readstat(siread *q, int cache, uint32_t reads)
{
	si *i = q->index;
	if (cache) {
		i->read_cache += reads;
		q->read_cache += reads;
	} else {
		i->read_disk += reads;
		q->read_disk += reads;
	}
}

static inline int
si_getresult(siread *q, char *v, int compare)
{
	int rc;
	if (compare) {
		rc = sf_compare(q->r->scheme, v, q->key);
		if (ssunlikely(rc != 0))
			return 0;
	}
	if (q->prefix) {
		rc = sf_compareprefix(q->r->scheme,
		                      q->prefix,
		                      q->prefix_size, v);
		if (ssunlikely(! rc))
			return 0;
	}
	if (ssunlikely(q->has))
		return sf_lsn(q->r->scheme, v) > q->vlsn;
	if (ssunlikely(sf_is(q->r->scheme, v, SVDELETE)))
		return 2;
	rc = si_readdup(q, v);
	if (ssunlikely(rc == -1))
		return -1;
	return 1;
}

static inline int
si_getindex(siread *q, sinode *n)
{
	svindex *second;
	svindex *first = si_nodeindex_priority(n, &second);
	ssiter i;
	ss_iterinit(sv_indexiter, &i);
	int rc;
	if (first->count > 0) {
		rc = ss_iteropen(sv_indexiter, &i, q->r, first, SS_GTE, q->key);
		if (rc) {
			goto result;
		}
	}
	if (sslikely(second == NULL || !second->count))
		return 0;
	rc = ss_iteropen(sv_indexiter, &i, q->r, second, SS_GTE, q->key);
	if (! rc) {
		return 0;
	}
result:;
	si_readstat(q, 1, 1);
	char *v = ss_iterof(sv_indexiter, &i);
	assert(v != NULL);
	svv *visible = (svv*)(v - sizeof(svv));
	if (sslikely(! q->has)) {
		visible = sv_vvisible(visible, q->r, q->vlsn);
		if (visible == NULL)
			return 0;
	}
	return si_getresult(q, v, 0);
}

static inline int
si_getfile(siread *q, sinode *n, sicache *c)
{
	sischeme *scheme = &q->index->scheme;
	int rc;
	/* choose compression type */
	sdreadarg arg = {
		.from_compaction     = 0,
		.io                  = &q->index->rdc.io,
		.index               = &n->index,
		.buf                 = &c->buf_a,
		.buf_read            = &q->index->rdc.d,
		.index_iter          = &c->index_iter,
		.page_iter           = &c->page_iter,
		.use_mmap            = scheme->mmap,
		.use_mmap_copy       = 0,
		.use_compression     = scheme->compression,
		.use_direct_io       = scheme->direct_io,
		.direct_io_page_size = scheme->direct_io_page_size,
		.compression_if      = scheme->compression_if,
		.has                 = q->has,
		.has_vlsn            = q->vlsn,
		.o                   = SS_GTE,
		.mmap                = &n->map,
		.file                = &n->file,
		.r                   = q->r
	};
	ss_iterinit(sd_read, &c->i);
	rc = ss_iteropen(sd_read, &c->i, &arg, q->key);
	int reads = sd_read_stat(&c->i);
	si_readstat(q, 0, reads);
	if (ssunlikely(rc <= 0))
		return rc;
	/* prepare sources */
	sv_mergereset(&q->merge);
	sv_mergeadd(&q->merge, &c->i);
	ssiter i;
	ss_iterinit(sv_mergeiter, &i);
	ss_iteropen(sv_mergeiter, &i, q->r, &q->merge, SS_GTE);
	uint64_t vlsn = q->vlsn;
	if (ssunlikely(q->has))
		vlsn = UINT64_MAX;
	ssiter j;
	ss_iterinit(sv_readiter, &j);
	ss_iteropen(sv_readiter, &j, q->r, &i, &q->index->rdc.upsert, vlsn, 1);
	char *v = ss_iterof(sv_readiter, &j);
	if (ssunlikely(v == NULL))
		return 0;
	return si_getresult(q, v, 1);
}

static inline int
si_get(siread *q)
{
	assert(q->key != NULL);
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, q->r, q->index, SS_GTE, q->key);
	sinode *node;
	node = ss_iterof(si_iter, &i);
	assert(node != NULL);

	/* search in memory */
	int rc;
	rc = si_getindex(q, node);
	if (rc != 0)
		return rc;
	sinodeview view;
	si_nodeview_open(&view, node);
	rc = si_cachevalidate(q->cache, node);
	if (ssunlikely(rc == -1)) {
		sr_oom(q->r->e);
		return -1;
	}
	si_unlock(q->index);

	/* search on disk */
	svmerge *m = &q->merge;
	rc = sv_mergeprepare(m, q->r, 1);
	assert(rc == 0);

	rc = si_getfile(q, node, q->cache);

	si_lock(q->index);
	si_nodeview_close(&view);
	return rc;
}

static inline int
si_rangefile(siread *q, sinode *n, svmerge *m)
{
	sicache *c = q->cache;
	assert(c->node == n);
	/* iterate cache */
	if (ss_iterhas(sd_read, &c->i)) {
		svmergesrc *s = sv_mergeadd(m, &c->i);
		si_readstat(q, 1, 1);
		s->ptr = c;
		return 1;
	}
	if (c->open) {
		return 1;
	}
	c->open = 1;
	/* choose compression type */
	sischeme *scheme = &q->index->scheme;
	sdreadarg arg = {
		.from_compaction     = 0,
		.io                  = &q->index->rdc.io,
		.index               = &n->index,
		.buf                 = &c->buf_a,
		.buf_read            = &q->index->rdc.d,
		.index_iter          = &c->index_iter,
		.page_iter           = &c->page_iter,
		.use_mmap            = scheme->mmap,
		.use_mmap_copy       = 1,
		.use_compression     = scheme->compression,
		.use_direct_io       = scheme->direct_io,
		.direct_io_page_size = scheme->direct_io_page_size,
		.compression_if      = scheme->compression_if,
		.has                 = 0,
		.has_vlsn            = 0,
		.o                   = q->order,
		.mmap                = &n->map,
		.file                = &n->file,
		.r                   = q->r
	};
	ss_iterinit(sd_read, &c->i);
	int rc = ss_iteropen(sd_read, &c->i, &arg, q->key);
	int reads = sd_read_stat(&c->i);
	si_readstat(q, 0, reads);
	if (ssunlikely(rc == -1))
		return -1;
	if (ssunlikely(! ss_iterhas(sd_read, &c->i)))
		return 0;
	svmergesrc *s = sv_mergeadd(m, &c->i);
	s->ptr = c;
	return 1;
}

static inline int
si_range(siread *q)
{
	assert(q->has == 0);

	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, q->r, q->index, q->order, q->key);
	sinode *node;
next_node:
	node = ss_iterof(si_iter, &i);
	if (ssunlikely(node == NULL))
		return 0;

	/* prepare sources */
	svmerge *m = &q->merge;
	int count = 1 + 2 + 1;
	int rc = sv_mergeprepare(m, q->r, count);
	if (ssunlikely(rc == -1)) {
		sr_errorreset(q->r->e);
		return -1;
	}

	/* include external upsert statement to the query */
	svmergesrc *s;
	ssbuf upsert_stream;
	char  upsert_stream_reserve[sizeof(char**)];
	if (ssunlikely(q->upsert)) {
		ss_bufinit_reserve(&upsert_stream, &upsert_stream_reserve,
		                   sizeof(upsert_stream_reserve));
		ss_bufadd(&upsert_stream, q->r->a, &q->upsert, sizeof(char**));
		s = sv_mergeadd(m, NULL);
		ss_iterinit(ss_bufiterref, &s->src);
		ss_iteropen(ss_bufiterref, &s->src, &upsert_stream, sizeof(char**));
	}

	/* in-memory indexes */
	svindex *second;
	svindex *first = si_nodeindex_priority(node, &second);
	if (first->count) {
		s = sv_mergeadd(m, NULL);
		ss_iterinit(sv_indexiter, &s->src);
		ss_iteropen(sv_indexiter, &s->src, q->r, first, q->order,
		            q->key);
	}
	if (ssunlikely(second && second->count)) {
		s = sv_mergeadd(m, NULL);
		ss_iterinit(sv_indexiter, &s->src);
		ss_iteropen(sv_indexiter, &s->src, q->r, second, q->order,
		            q->key);
	}

	/* read from file */
	rc = si_cachevalidate(q->cache, node);
	if (ssunlikely(rc == -1)) {
		sr_oom(q->r->e);
		return -1;
	}
	rc = si_rangefile(q, node, m);
	if (ssunlikely(rc == -1 || rc == 2))
		return rc;

	/* merge and filter data stream */
	ssiter j;
	ss_iterinit(sv_mergeiter, &j);
	ss_iteropen(sv_mergeiter, &j, q->r, m, q->order);
	ssiter k;
	ss_iterinit(sv_readiter, &k);
	ss_iteropen(sv_readiter, &k, q->r, &j, &q->index->rdc.upsert, q->vlsn, 0);
	char *v = ss_iterof(sv_readiter, &k);
	if (ssunlikely(v == NULL)) {
		sv_mergereset(&q->merge);
		ss_iternext(si_iter, &i);
		goto next_node;
	}

	rc = 1;
	/* convert upsert search to SS_EQ */
	if (q->upsert_eq) {
		rc = sf_compare(q->r->scheme, v, q->key);
		rc = rc == 0;
	}
	/* do prefix search */
	if (q->prefix && rc) {
		rc = sf_compareprefix(q->r->scheme, q->prefix,
		                      q->prefix_size, v);
	}
	if (sslikely(rc == 1)) {
		if (ssunlikely(si_readdup(q, v) == -1))
			return -1;
	}

	/* skip a possible duplicates from data sources */
	sv_readiter_forward(&k);
	return rc;
}

int si_read(siread *q)
{
	int rc;
	if (q->index->scheme.direct_io) {
		rc = sd_ioprepare(&q->index->rdc.io, q->r,
		                  q->index->scheme.direct_io,
		                  q->index->scheme.direct_io_page_size,
		                  q->index->scheme.direct_io_buffer_size);
		if (ssunlikely(rc == -1))
			return sr_oom(q->r->e);
	}
	switch (q->order) {
	case SS_EQ:
		return si_get(q);
	case SS_LT:
	case SS_LTE:
	case SS_GT:
	case SS_GTE:
		return si_range(q);
	default:
		break;
	}
	return -1;
}

int si_readcommited(si *index, sr *r, svv *v)
{
	/* search node index */
	ssiter i;
	ss_iterinit(si_iter, &i);
	ss_iteropen(si_iter, &i, r, index, SS_GTE, sv_vpointer(v));
	sinode *node;
	node = ss_iterof(si_iter, &i);
	assert(node != NULL);

	uint64_t lsn = sf_lsn(r->scheme, sv_vpointer(v));

	/* search index */
	ss_iterinit(sd_indexiter, &i);
	ss_iteropen(sd_indexiter, &i, r, &node->index, SS_GTE,
	            sv_vpointer(v));
	sdindexpage *page = ss_iterof(sd_indexiter, &i);
	if (page == NULL)
		return 0;
	if (page->lsnmax >= lsn)
		return 1;
	return 0;
}
