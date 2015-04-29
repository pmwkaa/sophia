
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

int sd_commitpage(sdbuild *b, sr *r, srbuf *buf)
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
} sdcommitiov;

static inline void
sd_commitiov_init(sdcommitiov *i, sdbuild *b, int iovmax)
{
	i->b = b;
	i->iovmax = iovmax;
	i->i = 0;
}

static inline int
sd_commitiov(sdcommitiov *i, sriov *iov)
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

int sd_commit(sdbuild *b, sr *r, sdindex *index, srfile *file)
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
	sdcommitiov iter;
	sd_commitiov_init(&iter, b, 1022);
	int more = 1;
	while (more) {
		more = sd_commitiov(&iter, &iov);
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
