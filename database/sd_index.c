
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

int sd_indexbegin(sdindex *i, sr *r, uint32_t keysize, uint64_t offset)
{
	int rc = sr_bufensure(&i->i, r->a, sizeof(sdindexheader));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdindexheader *h = sd_indexheader(i);
	sr_version(&h->version);
	h->crc       = 0;
	h->block     = sizeof(sdindexpage) + (keysize * 2);
	h->count     = 0;
	h->keys      = 0;
	h->total     = 0;
	h->totalkv   = 0;
	h->extension = 0;
	h->lsnmin    = UINT64_MAX;
	h->lsnmax    = 0;
	h->tsmin     = 0;
	h->offset    = offset;
	h->dupkeys   = 0;
	h->dupmin    = UINT64_MAX;
	memset(h->reserve, 0, sizeof(h->reserve));
	sd_idinit(&h->id, 0, 0, 0);
	i->h = h;
	sr_bufadvance(&i->i, sizeof(sdindexheader));
	return 0;
}

int sd_indexcommit(sdindex *i, sdid *id)
{
	i->h      = sd_indexheader(i);
	i->h->id  = *id;
	i->h->crc = sr_crcs(i->h, sizeof(sdindexheader), 0);
	return 0;
}

int sd_indexadd(sdindex *i, sr *r, uint64_t offset,
                uint32_t size,
                uint32_t sizekv,
                uint32_t count,
                char *min, int sizemin,
                char *max, int sizemax,
                uint32_t dupkeys,
                uint64_t dupmin,
                uint64_t lsnmin,
                uint64_t lsnmax)
{
	int rc = sr_bufensure(&i->i, r->a, i->h->block);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	i->h = sd_indexheader(i);
	sdindexpage *p = (sdindexpage*)i->i.p;
	p->offset   = offset;
	p->size     = size;
	p->sizemin  = sizemin;
	p->sizemax  = sizemax;
	p->lsnmin   = lsnmin;
	p->lsnmax   = lsnmax;
	memcpy(sd_indexpage_min(p), min, sizemin);
	memcpy(sd_indexpage_max(p), max, sizemax);
	memset(p->reserve, 0, sizeof(p->reserve));
	int padding = i->h->block - (sizeof(sdindexpage) + sizemin + sizemax);
	if (padding > 0)
		memset(sd_indexpage_max(p) + sizemax, 0, padding);
	i->h->count++;
	i->h->keys  += count;
	i->h->total += size;
	i->h->totalkv += sizekv;
	if (lsnmin < i->h->lsnmin)
		i->h->lsnmin = lsnmin;
	if (lsnmax > i->h->lsnmax)
		i->h->lsnmax = lsnmax;
	i->h->dupkeys += dupkeys;
	if (dupmin < i->h->dupmin)
		i->h->dupmin = dupmin;
	sr_bufadvance(&i->i, i->h->block);
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
	i->h = (sdindexheader*)i->i.s;
	return 0;
}
