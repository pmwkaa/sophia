
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

void sd_buildindex_init(sdbuildindex *i)
{
	ss_bufinit(&i->v);
	ss_bufinit(&i->m);
}

void sd_buildindex_free(sdbuildindex *i, sr *r)
{
	ss_buffree(&i->v, r->a);
	ss_buffree(&i->m, r->a);
}

void sd_buildindex_reset(sdbuildindex *i)
{
	ss_bufreset(&i->v);
	ss_bufreset(&i->m);
}

void sd_buildindex_gc(sdbuildindex *i, sr *r, int wm)
{
	ss_bufgc(&i->v, r->a, wm);
	ss_bufgc(&i->m, r->a, wm);
}

int sd_buildindex_begin(sdbuildindex *i)
{
	sdindexheader *h = &i->build;
	h->crc         = 0;
	h->size        = 0;
	h->sizevmax    = 0;
	h->count       = 0;
	h->keys        = 0;
	h->total       = 0;
	h->totalorigin = 0;
	h->lsnmin      = UINT64_MAX;
	h->lsnmax      = 0;
	h->tsmin       = UINT32_MAX;
	h->offset      = 0;
	h->dupkeys     = 0;
	h->dupmin      = UINT64_MAX;
	h->align       = 0;
	sr_version_storage(&h->version);
	sd_idinit(&h->id, 0, 0, 0);
	return 0;
}

int sd_buildindex_end(sdbuildindex *i, sr *r, sdid *id,
                      uint32_t align,
                      uint64_t offset)
{
	/* calculate index align for direct_io */
	int size_meta  = sizeof(sdindexheader);
	int size_align = 0;
	if (align) {
		size_align += align - ((offset +
		                        ss_bufused(&i->v) +
		                        size_meta +
		                        ss_bufused(&i->m)) % align);
		size_meta  += size_align;
	}
	int rc = ss_bufensure(&i->m, r->a, size_meta);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	/* align */
	sdindexheader *h = &i->build;
	if (size_align) {
		h->align = size_align;
		memset(i->m.p, 0, size_align);
		ss_bufadvance(&i->m, size_align);
	}
	/* header */
	h->offset = offset;
	h->id     = *id;
	h->crc = ss_crcs(r->crc, h, sizeof(sdindexheader), 0);
	memcpy(i->m.p, &i->build, sizeof(sdindexheader));
	ss_bufadvance(&i->m, sizeof(sdindexheader));
	return 0;
}

int sd_buildindex_add(sdbuildindex *i, sr *r, sdbuild *b, uint64_t offset)
{
	int rc = ss_bufensure(&i->m, r->a, sizeof(sdindexpage));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdpageheader *ph = sd_buildheader(b);

	int size = ph->size + sizeof(sdpageheader);
	int sizeorigin = ph->sizeorigin + sizeof(sdpageheader);

	/* prepare page header */
	sdindexpage *p = (sdindexpage*)i->m.p;
	p->offset      = offset;
	p->offsetindex = ss_bufused(&i->v);
	p->lsnmin      = ph->lsnmin;
	p->lsnmax      = ph->lsnmax;
	p->size        = size;
	p->sizeorigin  = sizeorigin;
	p->sizemin     = 0;
	p->sizemax     = 0;

	/* copy keys */
	if (ssunlikely(ph->count > 0)) {
		char *min = sd_buildmin(b, r);
		char *max = sd_buildmax(b, r);
		p->sizemin = sf_comparable_size(r->scheme, min);
		p->sizemax = sf_comparable_size(r->scheme, max);
		int rc = ss_bufensure(&i->v, r->a, p->sizemin + p->sizemax);
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
		sf_comparable_write(r->scheme, min, i->v.p);
		ss_bufadvance(&i->v, p->sizemin);
		sf_comparable_write(r->scheme, max, i->v.p);
		ss_bufadvance(&i->v, p->sizemax);
	}

	/* update index info */
	sdindexheader *h = &i->build;
	h->count++;
	h->size  += sizeof(sdindexpage) + p->sizemin + p->sizemax;
	h->keys  += ph->count;
	h->total += size;
	h->totalorigin += sizeorigin;
	if (b->vmax > h->sizevmax)
		h->sizevmax = b->vmax;
	if (ph->lsnmin < h->lsnmin)
		h->lsnmin = ph->lsnmin;
	if (ph->lsnmax > h->lsnmax)
		h->lsnmax = ph->lsnmax;
	if (ph->tsmin < h->tsmin)
		h->tsmin = ph->tsmin;
	h->dupkeys += ph->countdup;
	if (ph->lsnmindup < h->dupmin)
		h->dupmin = ph->lsnmindup;
	ss_bufadvance(&i->m, sizeof(sdindexpage));
	return 0;
}
