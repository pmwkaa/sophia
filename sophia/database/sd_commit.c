
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

int sd_commitpage(sdbuild *b, sr *r, ssbuf *buf)
{
	sdbuildref *ref = sd_buildref(b);
	/* compressed */
	uint32_t size = ss_bufused(&b->c);
	int rc;
	if (size > 0) {
		rc = ss_bufensure(buf, r->a, ref->csize);
		if (ssunlikely(rc == -1))
			return -1;
		memcpy(buf->p, b->c.s, ref->csize);
		ss_bufadvance(buf, ref->csize);
		return 0;
	}
	/* not compressed */
	assert(ref->msize != 0);
	int total = ref->msize + ref->vsize + ref->ksize;
	rc = ss_bufensure(buf, r->a, total);
	if (ssunlikely(rc == -1))
		return -1;
	memcpy(buf->p, b->m.s + ref->m, ref->msize);
	ss_bufadvance(buf, ref->msize);
	memcpy(buf->p, b->v.s + ref->v, ref->vsize);
	ss_bufadvance(buf, ref->vsize);
	memcpy(buf->p, b->k.s + ref->k, ref->ksize);
	ss_bufadvance(buf, ref->ksize);
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
sd_commitiov(sdcommitiov *i, ssiov *iov)
{
	uint32_t n = 0;
	while (i->i < i->b->n && n < (i->iovmax - 3)) {
		sdbuildref *ref =
			(sdbuildref*)ss_bufat(&i->b->list, sizeof(sdbuildref), i->i);
		ss_iovadd(iov, i->b->m.s + ref->m, ref->msize);
		ss_iovadd(iov, i->b->v.s + ref->v, ref->vsize);
		ss_iovadd(iov, i->b->k.s + ref->k, ref->ksize);
		i->i++;
		n += 3;
	}
	return i->i < i->b->n;
}

int sd_commit(sdbuild *b, sr *r, sdindex *index, ssfile *file)
{
	sdseal seal;
	sd_seal(&seal, r, index->h);
	struct iovec iovv[1024];
	ssiov iov;
	ss_iovinit(&iov, iovv, 1024);
	ss_iovadd(&iov, index->i.s, ss_bufused(&index->i));

	SS_INJECTION(r->i, SS_INJECTION_SD_BUILD_0,
	             sr_malfunction(r->e, "%s", "error injection");
	             assert( ss_filewritev(file, &iov) == 0 );
	             return -1);

	/* compression enabled */
	uint64_t start = file->size;
	uint32_t size = ss_bufused(&b->c);
	int rc;
	if (size > 0) {
		ss_iovadd(&iov, b->c.s, size);
		ss_iovadd(&iov, &seal, sizeof(seal));
		rc = ss_filewritev(file, &iov);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "file '%s' write error: %s",
			               file->file, strerror(errno));
			return -1;
		}
		goto done;
	}
	/* uncompressed */
	sdcommitiov iter;
	sd_commitiov_init(&iter, b, 1020);
	int more = 1;
	while (more) {
		more = sd_commitiov(&iter, &iov);
		if (sslikely(! more)) {
			SS_INJECTION(r->i, SS_INJECTION_SD_BUILD_1,
			             seal.crc++); /* corrupt seal */
			ss_iovadd(&iov, &seal, sizeof(seal));
		}
		rc = ss_filewritev(file, &iov);
		if (ssunlikely(rc == -1)) {
			return sr_malfunction(r->e, "file '%s' write error: %s",
			                      file->file, strerror(errno));
		}
		ss_iovreset(&iov);
	}
done:
	return file->size - start;
}

int sd_committo(sdbuild *b, sr *r, sdindex *index, char *dest, int size)
{
	sdseal seal;
	sd_seal(&seal, r, index->h);
	struct iovec iovv[1024];
	ssiov iov;
	ss_iovinit(&iov, iovv, 1024);
	ss_iovadd(&iov, index->i.s, ss_bufused(&index->i));
	char *p = dest;
	/* compression enabled */
	uint32_t csize = ss_bufused(&b->c);
	if (csize > 0) {
		ss_iovadd(&iov, b->c.s, csize);
		ss_iovadd(&iov, &seal, sizeof(seal));
		p = ss_iovwrite(&iov, p);
		assert(p <= (dest + size));
		return p - dest;
	}
	/* uncompressed */
	sdcommitiov iter;
	sd_commitiov_init(&iter, b, 1020);
	int more = 1;
	while (more) {
		more = sd_commitiov(&iter, &iov);
		if (sslikely(! more))
			ss_iovadd(&iov, &seal, sizeof(seal));
		p = ss_iovwrite(&iov, p);
		ss_iovreset(&iov);
	}
	assert(p <= (dest + size));
	return p - dest;
}
