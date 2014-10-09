
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

int sd_buildbegin(sdbuild *b, uint32_t keymax)
{
	int rc = sr_bufensure(&b->list, b->r->a, sizeof(sdbuildref));
	if (srunlikely(rc == -1))
		return -1;
	sdbuildref *ref =
		(sdbuildref*)sr_bufat(&b->list, sizeof(sdbuildref), b->n);
	ref->k     = sr_bufused(&b->k);
	ref->ksize = 0;
	ref->v     = sr_bufused(&b->v);
	ref->vsize = 0;
	rc = sr_bufensure(&b->k, b->r->a, sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		return -1;
	sdpageheader *h = sd_buildheader(b);
	memset(h, 0, sizeof(*h));
	h->sizeblock = sizeof(sdv) + keymax;
	h->lsnmin = (uint64_t)-1;
	h->lsnmax = 0;
	sr_bufadvance(&b->list, sizeof(sdbuildref));
	sr_bufadvance(&b->k, sizeof(sdpageheader));
	return 0;
}

int sd_buildcommit(sdbuild *b)
{
	sdbuildref *ref = sd_buildref(b);
	ref->ksize = sr_bufused(&b->k) - ref->k;
	ref->vsize = sr_bufused(&b->v) - ref->v;
	b->n++;
	return 0;
}

int sd_buildadd(sdbuild *b, sv *v)
{
	uint32_t sizeblock = sd_buildheader(b)->sizeblock;
	int rc = sr_bufensure(&b->k, b->r->a, sizeblock);
	if (srunlikely(rc == -1))
		return -1;
	sdpageheader *h = sd_buildheader(b);
	sdv *sv = (sdv*)b->k.p;
	if (sd_isdb(v)) {
		memcpy(sv, svraw(v), svrawsize(v));
	} else {
		sv->lsn       = svlsn(v);
		sv->flags     = svflags(v);
		sv->valuesize = svvaluesize(v);
		sv->keysize   = svkeysize(v);
		memcpy(sv->key, svkey(v), sv->keysize);
	}
	int padding = sizeblock - sizeof(sdv) - sv->keysize;
	if (padding > 0)
		memset(sv->key + sv->keysize, 0, padding);
	sv->flags = svflags(v); /* ensure v->flags */
	rc = sr_bufensure(&b->v, b->r->a, sv->valuesize);
	if (srunlikely(rc == -1))
		return -1;
	memcpy(b->v.p, svvalue(v), sv->valuesize);
	sv->valueoffset =
		sr_bufused(&b->v) - sd_buildref(b)->v;
	uint32_t crc;
	crc = sr_crcp(sv->key, sv->keysize, 0);
	crc = sr_crcp(b->v.p, sv->valuesize, crc);
	crc = sr_crcs(sv, sizeof(sdv), crc);
	sv->crc = crc;
	h->count++;
	h->size   += sv->valuesize + sizeblock;
	h->sizekv += sv->keysize + sv->valuesize;
	if (sv->lsn > h->lsnmax)
		h->lsnmax = sv->lsn;
	if (sv->lsn < h->lsnmin)
		h->lsnmin = sv->lsn;
	sr_bufadvance(&b->k, sizeblock);
	sr_bufadvance(&b->v, sv->valuesize);
	return 0;
}

int sd_buildend(sdbuild *b)
{
	sdpageheader *h = sd_buildheader(b);
	h->crc = sr_crcs(h, sizeof(sdpageheader), 0);
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
	srversion v;
	sr_version(&v);

	struct iovec iovv[1024];
	sriov iov;
	sr_iovinit(&iov, iovv, 1024);
	sr_iovadd(&iov, &v, sizeof(v));
	sdbuildiov iter;
	sd_buildiov_init(&iter, b, 1022);
	int more = 1;
	while (more) {
		more = sd_buildiov(&iter, &iov);
		if (srlikely(! more))
			sr_iovadd(&iov, index->i.s, sr_bufused(&index->i));
		int rc = sr_filewritev(file, &iov);
		if (srunlikely(rc == -1))
			return -1;
		sr_iovreset(&iov);
	}
	/* sync? */
	return 0;
}
