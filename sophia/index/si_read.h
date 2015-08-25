#ifndef SI_READ_H_
#define SI_READ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siread siread;
typedef struct sireadarg sireadarg;

struct sireadarg {
	sischeme *scheme;
	si       *index;
	sinode   *n;
	sibranch *b;
	ssbuf    *buf;
	ssbuf    *buf_xf;
	ssbuf    *buf_read;
	ssiter   *index_iter;
	ssiter   *page_iter;
	uint64_t  vlsn;
	int       has;
	int       mmap_copy;
	ssorder   o;
	sr       *r;
};

struct siread {
	sireadarg ra;
	sdindexpage *ref;
	sdpage page;
} sspacked;

static inline int
si_read_page(siread *i, sdindexpage *ref)
{
	sireadarg *arg = &i->ra;
	sr *r = arg->r;

	arg->index->read_disk++;

	ss_bufreset(arg->buf_xf);
	int rc = ss_bufensure(arg->buf_xf, r->a, arg->b->index.h->sizevmax);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	ss_bufreset(arg->buf);
	rc = ss_bufensure(arg->buf, r->a, ref->sizeorigin);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);

	uint64_t offset =
		arg->b->index.h->offset + sd_indexsize(arg->b->index.h) +
		ref->offset;

	/* compression */
	if (i->ra.scheme->compression)
	{
		char *page_pointer;
		if (i->ra.scheme->in_memory) {
			offset -= arg->b->index.h->offset;
			page_pointer = arg->b->copy.p + offset;
		} else
		if (i->ra.scheme->mmap) {
			page_pointer = arg->n->map.p + offset;
		} else {
			ss_bufreset(arg->buf_read);
			rc = ss_bufensure(arg->buf_read, r->a, ref->size);
			if (ssunlikely(rc == -1))
				return sr_oom(r->e);
			rc = ss_filepread(&arg->n->file, offset, arg->buf_read->s, ref->size);
			if (ssunlikely(rc == -1)) {
				sr_error(r->e, "db file '%s' read error: %s",
				         arg->n->file.file, strerror(errno));
				return -1;
			}
			ss_bufadvance(arg->buf_read, ref->size);
			page_pointer = arg->buf_read->s;
		}

		/* copy header */
		memcpy(arg->buf->p, page_pointer, sizeof(sdpageheader));
		ss_bufadvance(arg->buf, sizeof(sdpageheader));

		/* decompression */
		ssfilter f;
		rc = ss_filterinit(&f, (ssfilterif*)r->compression, r->a, SS_FOUTPUT);
		if (ssunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' decompression error", arg->n->file.file);
			return -1;
		}
		int size = ref->size - sizeof(sdpageheader);
		rc = ss_filternext(&f, arg->buf, page_pointer + sizeof(sdpageheader), size);
		if (ssunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' decompression error", arg->n->file.file);
			return -1;
		}
		ss_filterfree(&f);
		sd_pageinit(&i->page, (sdpageheader*)arg->buf->s);
		return 0;
	}

	/* in-memory mode */
	if (i->ra.scheme->in_memory) {
		offset -= arg->b->index.h->offset;
		sd_pageinit(&i->page, (sdpageheader*)(arg->b->copy.p + offset));
		return 0;
	}

	/* mmap */
	if (i->ra.scheme->mmap) {
		if (i->ra.mmap_copy) {
			memcpy(arg->buf->s, arg->n->map.p + offset, ref->sizeorigin);
			sd_pageinit(&i->page, (sdpageheader*)(arg->buf->s));
		} else {
			sd_pageinit(&i->page, (sdpageheader*)(arg->n->map.p + offset));
		}
		return 0;
	}

	/* default */
	rc = ss_filepread(&arg->n->file, offset, arg->buf->s, ref->sizeorigin);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
		         arg->n->file.file, strerror(errno));
		return -1;
	}
	ss_bufadvance(arg->buf, ref->sizeorigin);
	sd_pageinit(&i->page, (sdpageheader*)(arg->buf->s));
	return 0;
}

static inline int
si_read_openpage(siread *i, void *key, int keysize)
{
	sireadarg *arg = &i->ra;
	assert(i->ref != NULL);
	int rc = si_read_page(i, i->ref);
	if (ssunlikely(rc == -1))
		return -1;
	ss_iterinit(sd_pageiter, arg->page_iter);
	return ss_iteropen(sd_pageiter, arg->page_iter, arg->r,
	                   arg->buf_xf,
	                   &i->page, arg->o, key, keysize);
}

static inline void
si_read_next(ssiter*);

static inline int
si_read_open(ssiter *iptr, sireadarg *arg, void *key, int keysize)
{
	siread *i = (siread*)iptr->priv;
	i->ra = *arg;
	ss_iterinit(sd_indexiter, arg->index_iter);
	ss_iteropen(sd_indexiter, arg->index_iter, arg->r, &arg->b->index,
	            arg->o, key, keysize);
	i->ref = ss_iterof(sd_indexiter, arg->index_iter);
	if (i->ref == NULL)
		return 0;
	if (ssunlikely(arg->has)) {
		assert(arg->o == SS_GTE);
		if (i->ref->lsnmax <= arg->vlsn) {
			i->ref = NULL;
			return 0;
		}
	}
	int rc = si_read_openpage(i, key, keysize);
	if (ssunlikely(rc == -1)) {
		i->ref = NULL;
		return -1;
	}
	if (ssunlikely(! ss_iterhas(sd_pageiter, i->ra.page_iter))) {
		si_read_next(iptr);
		rc = 0;
	}
	return rc;
}

static inline void
si_read_close(ssiter *iptr)
{
	siread *i = (siread*)iptr->priv;
	i->ref = NULL;
}

static inline int
si_read_has(ssiter *iptr)
{
	siread *i = (siread*)iptr->priv;
	if (ssunlikely(i->ref == NULL))
		return 0;
	return ss_iterhas(sd_pageiter, i->ra.page_iter);
}

static inline void*
si_read_of(ssiter *iptr)
{
	siread *i = (siread*)iptr->priv;
	if (ssunlikely(i->ref == NULL))
		return NULL;
	return ss_iterof(sd_pageiter, i->ra.page_iter);
}

static inline void
si_read_next(ssiter *iptr)
{
	siread *i = (siread*)iptr->priv;
	if (ssunlikely(i->ref == NULL))
		return;
	ss_iternext(sd_pageiter, i->ra.page_iter);
retry:
	if (sslikely(ss_iterhas(sd_pageiter, i->ra.page_iter)))
		return;
	ss_iternext(sd_indexiter, i->ra.index_iter);
	i->ref = ss_iterof(sd_indexiter, i->ra.index_iter);
	if (i->ref == NULL)
		return;
	int rc = si_read_openpage(i, NULL, 0);
	if (ssunlikely(rc == -1)) {
		i->ref = NULL;
		return;
	}
	goto retry;
}

extern ssiterif si_read;

#endif
