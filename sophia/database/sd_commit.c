
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

int sd_writeseal(sr *r, ssfile *file, ssblob *blob)
{
	sdseal seal;
	sd_sealset_open(&seal, r);
	SS_INJECTION(r->i, SS_INJECTION_SD_BUILD_1,
	             seal.crc++); /* corrupt seal */
	int rc;
	rc = ss_filewrite(file, &seal, sizeof(seal));
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               file->file, strerror(errno));
		return -1;
	}
	if (blob) {
		rc = ss_blobadd(blob, &seal, sizeof(seal));
		if (ssunlikely(rc == -1))
			return sr_oom_malfunction(r->e);
	}
	return 0;
}

int sd_writepage(sr *r, ssfile *file, ssblob *blob, sdbuild *b)
{
	SS_INJECTION(r->i, SS_INJECTION_SD_BUILD_0,
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);
	sdbuildref *ref = sd_buildref(b);
	struct iovec iovv[3];
	ssiov iov;
	ss_iovinit(&iov, iovv, 3);
	if (ss_bufused(&b->c) > 0) {
		/* compressed */
		ss_iovadd(&iov, b->c.s, ref->csize);
	} else {
		/* uncompressed */
		ss_iovadd(&iov, b->m.s + ref->m, ref->msize);
		ss_iovadd(&iov, b->v.s + ref->v, ref->vsize);
		ss_iovadd(&iov, b->k.s + ref->k, ref->ksize);
	}
	int rc;
	rc = ss_filewritev(file, &iov);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               file->file, strerror(errno));
		return -1;
	}
	if (blob) {
		int i = 0;
		while (i < iov.iovc) {
			struct iovec *v = &iovv[i];
			rc = ss_blobadd(blob, v->iov_base, v->iov_len);
			if (ssunlikely(rc == -1))
				return sr_oom_malfunction(r->e);
			i++;
		}
	}
	return 0;
}

int sd_writeindex(sr *r, ssfile *file, ssblob *blob, sdindex *index)
{
	int rc;
	rc = ss_filewrite(file, index->i.s, ss_bufused(&index->i));
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               file->file, strerror(errno));
		return -1;
	}
	if (blob) {
		rc = ss_blobadd(blob, index->i.s, ss_bufused(&index->i));
		if (ssunlikely(rc == -1))
			return sr_oom_malfunction(r->e);
	}
	return 0;
}

int sd_seal(sr *r, ssfile *file, ssblob *blob, sdindex *index, uint64_t offset)
{
	sdseal seal;
	sd_sealset_close(&seal, r, index->h);
	int rc;
	rc = ss_filepwrite(file, offset, &seal, sizeof(seal));
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               file->file, strerror(errno));
		return -1;
	}
	if (blob) {
		assert(blob->map.size >= sizeof(seal));
		memcpy(blob->map.p, &seal, sizeof(seal));
	}
	return 0;
}
