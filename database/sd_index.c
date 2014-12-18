
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

int sd_indexbegin(sdindex *i, sr *r, uint32_t keysize)
{
	int rc = sr_bufensure(&i->i, r->a, sizeof(sdindexheader));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdindexheader *h = sd_indexheader(i);
	i->h         = h;
	h->crc       = 0;
	h->block     = sizeof(sdindexpage) + (keysize * 2);
	h->count     = 0;
	h->keys      = 0;
	h->total     = 0;
	h->totalkv   = 0;
	h->lsnmin    = 0;
	h->lsnmax    = 0;
	sd_idinit(&h->id, 0, 0, 0);
	sr_bufadvance(&i->i, sizeof(sdindexheader));
	return 0;
}

int sd_indexcommit(sdindex *i, sr *r, sdid *id)
{
	int rc = sr_bufensure(&i->i, r->a, sizeof(sdindexfooter));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sr_bufadvance(&i->i, sizeof(sdindexfooter));
	i->h        = sd_indexheader(i);
	i->h->id    = *id;
	i->h->crc   = sr_crcs(i->h, sizeof(sdindexheader), 0);
	i->f        = sd_indexfooter(i);
	i->f->magic = 0x12345;
	i->f->size  = sr_bufused(&i->i);
	i->f->crc   = sr_crcs(i->f, sizeof(sdindexfooter), i->h->crc);
	return 0;
}

int sd_indexadd(sdindex *i, sr *r, uint32_t offset,
                uint32_t size,
                uint32_t sizekv,
                uint32_t count,
                char *min, int sizemin,
                char *max, int sizemax,
                uint64_t lsnmin,
                uint64_t lsnmax)
{
	int rc = sr_bufensure(&i->i, r->a, i->h->block);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	i->h = sd_indexheader(i);
	sdindexpage *p = (sdindexpage*)i->i.p;
	p->offset  = offset;
	p->size    = size;
	p->sizemin = sizemin;
	p->sizemax = sizemax;
	p->lsnmin  = lsnmin;
	p->lsnmax  = lsnmax;
	memcpy(sd_indexpage_min(p), min, sizemin);
	memcpy(sd_indexpage_max(p), max, sizemax);
	int padding = i->h->block - (sizeof(sdindexpage) + sizemin + sizemax);
	if (padding > 0)
		memset(sd_indexpage_max(p) + sizemax, 0, padding);
	i->h->count++;
	i->h->keys  += count;
	i->h->total += size;
	i->h->totalkv += sizekv;
	if (lsnmin > i->h->lsnmin)
		i->h->lsnmin = lsnmin;
	if (lsnmax > i->h->lsnmax)
		i->h->lsnmax = lsnmax;
	sr_bufadvance(&i->i, i->h->block);
	return 0;
}

sdindexheader*
sd_indexvalidate(srmap *map, sr *r)
{
	uint32_t minsize =
	    sizeof(srversion) + sizeof(sdindexheader) +
	    sizeof(sdindexfooter);
	if (map->size < minsize) {
		sr_error(r->e, "%s", "bad index size");
		return NULL;
	}
	sdindexfooter *f = (sdindexfooter*)
		(map->p + (map->size - sizeof(sdindexfooter)));
	sdindexheader *h =
		(sdindexheader*)(map->p + (map->size - f->size));
	uint32_t crc = sr_crcs(h, sizeof(sdindexheader), 0);
	if (h->crc != crc) {
		sr_error(r->e, "%s", "bad index header crc");
		return NULL;
	}
	crc = sr_crcs(f, sizeof(sdindexfooter), h->crc);
	if (f->crc != crc) {
		sr_error(r->e, "%s", "bad index footer crc");
		return NULL;
	}
	return h;
}

int sd_indexrecover(sdindex *i, sr *r, srmap *map)
{
	sdindexheader *h = sd_indexvalidate(map, r);
	if (srunlikely(h == NULL))
		return -1;
	uint32_t size =
		sizeof(sdindexheader) + h->count * h->block +
		sizeof(sdindexfooter);
	int rc = sr_bufensure(&i->i, r->a, size);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	memcpy(i->i.s, (char*)h, size);
	sr_bufadvance(&i->i, size);
	i->h = sd_indexheader(i);
	return 0;
}
