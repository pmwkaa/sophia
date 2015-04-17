
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

int sd_buildbegin(sdbuild *b, sr *r, int crc, int compress)
{
	b->crc = crc;
	b->compress = compress;
	int rc = sr_bufensure(&b->list, r->a, sizeof(sdbuildref));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdbuildref *ref =
		(sdbuildref*)sr_bufat(&b->list, sizeof(sdbuildref), b->n);
	ref->k     = sr_bufused(&b->k);
	ref->ksize = 0;
	ref->v     = sr_bufused(&b->v);
	ref->vsize = 0;
	ref->c     = sr_bufused(&b->c);
	ref->csize = 0;
	rc = sr_bufensure(&b->k, r->a, sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdpageheader *h = sd_buildheader(b);
	memset(h, 0, sizeof(*h));
	h->lsnmin     = UINT64_MAX;
	h->lsnmindup  = UINT64_MAX;
	h->tsmin      = 0;
	memset(h->reserve, 0, sizeof(h->reserve));
	sr_bufadvance(&b->list, sizeof(sdbuildref));
	sr_bufadvance(&b->k, sizeof(sdpageheader));
	return 0;
}

int sd_buildadd(sdbuild *b, sr *r, sv *v, uint32_t flags)
{
	/* prepare metadata reference */
	int rc = sr_bufensure(&b->k, r->a, sizeof(sdv));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdpageheader *h = sd_buildheader(b);
	sdv *sv = (sdv*)b->k.p;
	sv->lsn       = sv_lsn(v);
	sv->flags     = sv_flags(v) | flags;
	sv->timestamp = 0;
	sv->size      = sv_size(v);
	sv->offset    = sr_bufused(&b->v) - sd_buildref(b)->v;
	/* copy object */
	rc = sr_bufensure(&b->v, r->a, sv->size);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	memcpy(b->v.p, sv_pointer(v), sv->size);
	sr_bufadvance(&b->v, sv->size);
	sr_bufadvance(&b->k, sizeof(sdv));
	/* update page header */
	h->count++;
	h->size +=  sv->size + sizeof(sdv);
	if (sv->lsn > h->lsnmax)
		h->lsnmax = sv->lsn;
	if (sv->lsn < h->lsnmin)
		h->lsnmin = sv->lsn;
	if (sv->flags & SVDUP) {
		h->countdup++;
		if (sv->lsn < h->lsnmindup)
			h->lsnmindup = sv->lsn;
	}
	return 0;
}

static inline int
sd_buildcompress(sdbuild *b, sr *r)
{
	/* reserve header */
	int rc = sr_bufensure(&b->c, r->a, sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		return -1;
	sr_bufadvance(&b->c, sizeof(sdpageheader));
	/* compression (including meta-data) */
	sdbuildref *ref = sd_buildref(b);
	srfilter f;
	rc = sr_filterinit(&f, (srfilterif*)r->compression, r, SR_FINPUT);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_filterstart(&f, &b->c);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filternext(&f, &b->c, b->k.s + ref->k + sizeof(sdpageheader),
	                   ref->ksize - sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filternext(&f, &b->c, b->v.s + ref->v, ref->vsize);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filtercomplete(&f, &b->c);
	if (srunlikely(rc == -1))
		goto error;
	sr_filterfree(&f);
	return 0;
error:
	sr_filterfree(&f);
	return -1;
}

int sd_buildend(sdbuild *b, sr *r)
{
	/* update sizes */
	sdbuildref *ref = sd_buildref(b);
	ref->ksize = sr_bufused(&b->k) - ref->k;
	ref->vsize = sr_bufused(&b->v) - ref->v;
	ref->csize = 0;
	/* calculate data crc (non-compressed) */
	sdpageheader *h = sd_buildheader(b);
	uint32_t crc = 0;
	if (srlikely(b->crc)) {
		crc = sr_crcp(r->crc, b->k.s + ref->k, ref->ksize, 0);
		crc = sr_crcp(r->crc, b->v.s + ref->v, ref->vsize, crc);
	}
	h->crcdata = crc;
	/* compression */
	if (b->compress) {
		int rc = sd_buildcompress(b, r);
		if (srunlikely(rc == -1))
			return -1;
		ref->csize = sr_bufused(&b->c) - ref->c;
	}
	/* update page header */
	h->sizeorigin = h->size;
	if (b->compress)
		h->size = ref->csize - sizeof(sdpageheader);
	h->crc = sr_crcs(r->crc, h, sizeof(sdpageheader), 0);
	if (b->compress)
		memcpy(b->c.s + ref->c, h, sizeof(sdpageheader));
	return 0;
}

int sd_buildcommit(sdbuild *b)
{
	/* if in compression, reset kv */
	if (b->compress) {
		sr_bufreset(&b->k);
		sr_bufreset(&b->v);
	}
	b->n++;
	return 0;
}

int sd_buildwritepage(sdbuild *b, sr *r, srbuf *buf)
{
	sdbuildref *ref = sd_buildref(b);
	/* compressed */
	uint32_t size = sr_bufused(&b->c);
	int rc;
	if (size > 0) {
		rc = sr_bufensure(buf, r->a, ref->csize);
		if (srunlikely(rc == -1))
			return -1;
		memcpy(buf->p, b->c.s, ref->csize);
		sr_bufadvance(buf, ref->csize);
		return 0;
	}
	/* not compressed */
	assert(ref->ksize != 0);
	rc = sr_bufensure(buf, r->a, ref->ksize + ref->vsize);
	if (srunlikely(rc == -1))
		return -1;
	memcpy(buf->p, b->k.s + ref->k, ref->ksize);
	sr_bufadvance(buf, ref->ksize);
	memcpy(buf->p, b->v.s + ref->v, ref->vsize);
	sr_bufadvance(buf, ref->vsize);
	return 0;
}

typedef struct {
	sdbuild *b;
	uint32_t i;
	uint32_t iovmax;
} sdbuildiov;

static inline void
sd_buildiov_init(sdbuildiov *i, sdbuild *b, int iovmax)
{
	i->b = b;
	i->iovmax = iovmax;
	i->i = 0;
}

static inline int
sd_buildiov(sdbuildiov *i, sriov *iov)
{
	uint32_t n = 0;
	while (i->i < i->b->n && n < (i->iovmax-2)) {
		sdbuildref *ref =
			(sdbuildref*)sr_bufat(&i->b->list, sizeof(sdbuildref), i->i);
		sr_iovadd(iov, i->b->k.s + ref->k, ref->ksize);
		sr_iovadd(iov, i->b->v.s + ref->v, ref->vsize);
		i->i++;
		n += 2;
	}
	return i->i < i->b->n;
}

int sd_buildwrite(sdbuild *b, sr *r, sdindex *index, srfile *file)
{
	sdseal seal;
	sd_seal(&seal, r, index->h);
	struct iovec iovv[1024];
	sriov iov;
	sr_iovinit(&iov, iovv, 1024);
	sr_iovadd(&iov, index->i.s, sr_bufused(&index->i));

	SR_INJECTION(r->i, SR_INJECTION_SD_BUILD_0,
	             sr_malfunction(r->e, "%s", "error injection");
	             assert( sr_filewritev(file, &iov) == 0 );
	             return -1);

	/* compression enabled */
	uint32_t size = sr_bufused(&b->c);
	int rc;
	if (size > 0) {
		sr_iovadd(&iov, b->c.s, size);
		sr_iovadd(&iov, &seal, sizeof(seal));
		rc = sr_filewritev(file, &iov);
		if (srunlikely(rc == -1))
			sr_malfunction(r->e, "file '%s' write error: %s",
			               file->file, strerror(errno));
		return rc;
	}

	/* uncompressed */
	sdbuildiov iter;
	sd_buildiov_init(&iter, b, 1022);
	int more = 1;
	while (more) {
		more = sd_buildiov(&iter, &iov);
		if (srlikely(! more)) {
			SR_INJECTION(r->i, SR_INJECTION_SD_BUILD_1,
			             seal.crc++); /* corrupt seal */
			sr_iovadd(&iov, &seal, sizeof(seal));
		}
		rc = sr_filewritev(file, &iov);
		if (srunlikely(rc == -1)) {
			return sr_malfunction(r->e, "file '%s' write error: %s",
			                      file->file, strerror(errno));
		}
		sr_iovreset(&iov);
	}
	return 0;
}
