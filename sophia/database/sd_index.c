
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

int sd_indexbegin(sdindex *i, sr *r, uint64_t offset)
{
	int rc = sr_bufensure(&i->i, r->a, sizeof(sdindexheader));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdindexheader *h = sd_indexheader(i);
	sr_version(&h->version);
	h->crc         = 0;
	h->size        = 0;
	h->count       = 0;
	h->keys        = 0;
	h->total       = 0;
	h->totalorigin = 0;
	h->extension   = 0;
	h->lsnmin      = UINT64_MAX;
	h->lsnmax      = 0;
	h->tsmin       = 0;
	h->offset      = offset;
	h->dupkeys     = 0;
	h->dupmin      = UINT64_MAX;
	memset(h->reserve, 0, sizeof(h->reserve));
	sd_idinit(&h->id, 0, 0, 0);
	i->h = NULL;
	sr_bufadvance(&i->i, sizeof(sdindexheader));
	return 0;
}

int sd_indexcommit(sdindex *i, sr *r, sdid *id)
{
	int size = sr_bufused(&i->v);
	int rc = sr_bufensure(&i->i, r->a, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	memcpy(i->i.p, i->v.s, size);
	sr_bufadvance(&i->i, size);
	sr_buffree(&i->v, r->a);
	i->h      = sd_indexheader(i);
	i->h->id  = *id;
	i->h->crc = sr_crcs(r->crc, i->h, sizeof(sdindexheader), 0);
	return 0;
}

int sd_indexadd(sdindex *i, sr *r, sdbuild *build)
{
	int rc = sr_bufensure(&i->i, r->a, sizeof(sdindexpage));
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	sdpageheader *ph = sd_buildheader(build);

	int size = ph->size + sizeof(sdpageheader);
	int sizeorigin = ph->sizeorigin + sizeof(sdpageheader);


	/* prepare page header.
	 *
	 * offset is relative to index:
	 * m->offset + (index_size) + page->offset
	*/
	sdindexpage *p = (sdindexpage*)i->i.p;
	p->offset      = sd_buildoffset(build);
	p->offsetindex = sr_bufused(&i->v);
	p->lsnmin      = ph->lsnmin;
	p->lsnmax      = ph->lsnmax;
	p->size        = size;
	p->sizeorigin  = sizeorigin;
	unsigned char *minptr = NULL;
	unsigned char *maxptr = NULL;
	if (srlikely(ph->count > 0)) {
		uint64_t unused;
		minptr  = (unsigned char*)sd_buildminkey(build);
		minptr += sr_leb128read(minptr, &unused);
		minptr += sr_leb128read(minptr, &unused);
		maxptr  = (unsigned char*)sd_buildmaxkey(build);
		maxptr += sr_leb128read(maxptr, &unused);
		maxptr += sr_leb128read(maxptr, &unused);
		p->sizemin = sr_formatkey_total(r->cmp, (char*)minptr);
		p->sizemax = sr_formatkey_total(r->cmp, (char*)maxptr);
	} else {
		minptr = NULL;
		maxptr = NULL;
		p->sizemin = 0;
		p->sizemax = 0;
	}
	/* update index info */
	sdindexheader *h = sd_indexheader(i);
	h->count++;
	h->size  += sizeof(sdindexpage) + p->sizemin + p->sizemax;
	h->keys  += ph->count;
	h->total += size;
	h->totalorigin += sizeorigin;
	if (ph->lsnmin < h->lsnmin)
		h->lsnmin = ph->lsnmin;
	if (ph->lsnmax > h->lsnmax)
		h->lsnmax = ph->lsnmax;
	h->dupkeys += ph->countdup;
	if (ph->lsnmindup < h->dupmin)
		h->dupmin = ph->lsnmindup;
	sr_bufadvance(&i->i, sizeof(sdindexpage));

	if (srunlikely(ph->count == 0))
		return 0;

	/* copy keys */
	rc = sr_bufensure(&i->v, r->a, p->sizemin + p->sizemax);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	/* reformat key object to exclude value */
	rc = sr_formatkey_copy(r->cmp, i->v.p, (char*)minptr);
	assert(rc == p->sizemin);
	(void)rc;
	sr_bufadvance(&i->v, p->sizemin);
	rc = sr_formatkey_copy(r->cmp, i->v.p, (char*)maxptr);
	assert(rc == p->sizemax);
	(void)rc;
	sr_bufadvance(&i->v, p->sizemax);
	return 0;
}

int sd_indexcopy(sdindex *i, sr *r, sdindexheader *h)
{
	int size = sd_indexsize(h);
	int rc = sr_bufensure(&i->i, r->a, size);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	memcpy(i->i.s, (char*)h, size);
	sr_bufadvance(&i->i, size);
	i->h = sd_indexheader(i);
	return 0;
}
