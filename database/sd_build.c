
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

int sd_buildbegin(sdbuild *b)
{
	int rc = sr_bufensure(&b->list, b->r->a, sizeof(sdbuildref));
	if (srunlikely(rc == -1))
		return sr_error(b->r->e, "%s", "memory allocation failed");
	sdbuildref *ref =
		(sdbuildref*)sr_bufat(&b->list, sizeof(sdbuildref), b->n);
	ref->k     = sr_bufused(&b->k);
	ref->ksize = 0;
	ref->v     = sr_bufused(&b->v);
	ref->vsize = 0;
	rc = sr_bufensure(&b->k, b->r->a, sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		return sr_error(b->r->e, "%s", "memory allocation failed");
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

int sd_buildadd(sdbuild *b, sv *v, uint32_t flags)
{
	/* prepare metadata reference */
	int rc = sr_bufensure(&b->k, b->r->a, sizeof(sdv));
	if (srunlikely(rc == -1))
		return sr_error(b->r->e, "%s", "memory allocation failed");
	sdpageheader *h = sd_buildheader(b);
	sdv *sv = (sdv*)b->k.p;
	sv->lsn         = svlsn(v);
	sv->flags       = svflags(v) | flags;
	sv->timestamp   = 0;
	sv->reserve     = 0;
	sv->keysize     = svkeysize(v);
	sv->valuesize   = svvaluesize(v);
	sv->keyoffset   = sr_bufused(&b->v) - sd_buildref(b)->v;
	sv->valueoffset = sv->keyoffset + sv->keysize;
	/* copy key-value pair */
	rc = sr_bufensure(&b->v, b->r->a, sv->keysize + sv->valuesize);
	if (srunlikely(rc == -1))
		return sr_error(b->r->e, "%s", "memory allocation failed");
	char *data = b->v.p;
	memcpy(b->v.p, svkey(v), sv->keysize);
	sr_bufadvance(&b->v, sv->keysize);
	memcpy(b->v.p, svvalue(v), sv->valuesize);
	sr_bufadvance(&b->v, sv->valuesize);
	sr_bufadvance(&b->k, sizeof(sdv));
	uint32_t crc;
	crc = sr_crcp(data, sv->keysize + sv->valuesize, 0);
	crc = sr_crcs(sv, sizeof(sdv), crc);
	sv->crc = crc;
	/* update page header */
	h->count++;
	h->size += sv->keysize + sv->valuesize + sizeof(sdv);
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

int sd_buildend(sdbuild *b)
{
	sdpageheader *h = sd_buildheader(b);
	h->crc = sr_crcs(h, sizeof(sdpageheader), 0);
	sdbuildref *ref = sd_buildref(b);
	ref->ksize = sr_bufused(&b->k) - ref->k;
	ref->vsize = sr_bufused(&b->v) - ref->v;
	return 0;
}

int sd_buildcommit(sdbuild *b)
{
	b->n++;
	return 0;
}

int sd_buildwritepage(sdbuild *b, srbuf *buf)
{
	sdbuildref *ref = sd_buildref(b);
	assert(ref->ksize != 0);
	int rc = sr_bufensure(buf, b->r->a, ref->ksize + ref->vsize);
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

int sd_buildwrite(sdbuild *b, sdindex *index, srfile *file)
{
	sdseal seal;
	sd_seal(&seal, index->h);
	struct iovec iovv[1024];
	sriov iov;
	sr_iovinit(&iov, iovv, 1024);
	sr_iovadd(&iov, index->i.s, sr_bufused(&index->i));

	SR_INJECTION(b->r->i, SR_INJECTION_SD_BUILD_0,
	             sr_malfunction(b->r->e, "%s", "error injection");
	             assert( sr_filewritev(file, &iov) == 0 );
	             return -1);

	sdbuildiov iter;
	sd_buildiov_init(&iter, b, 1022);
	int more = 1;
	while (more) {
		more = sd_buildiov(&iter, &iov);
		if (srlikely(! more)) {
			SR_INJECTION(b->r->i, SR_INJECTION_SD_BUILD_1,
			             seal.crc++); /* corrupt seal */
			sr_iovadd(&iov, &seal, sizeof(seal));
		}
		int rc;
		rc = sr_filewritev(file, &iov);
		if (srunlikely(rc == -1)) {
			return sr_malfunction(b->r->e, "file '%s' write error: %s",
			                      file->file, strerror(errno));
		}
		sr_iovreset(&iov);
	}
	return 0;
}
