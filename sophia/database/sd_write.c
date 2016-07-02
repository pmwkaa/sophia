
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
	/* compressed */
	uint32_t size = ss_bufused(&b->c);
	int rc;
	if (size > 0) {
		rc = ss_bufensure(buf, r->a, ss_bufused(&b->c));
		if (ssunlikely(rc == -1))
			return -1;
		memcpy(buf->p, b->c.s, ss_bufused(&b->c));
		ss_bufadvance(buf, ss_bufused(&b->c));
		return 0;
	}
	/* not compressed */
	int total = ss_bufused(&b->m) + ss_bufused(&b->v);
	rc = ss_bufensure(buf, r->a, total);
	if (ssunlikely(rc == -1))
		return -1;
	memcpy(buf->p, b->m.s, ss_bufused(&b->m));
	ss_bufadvance(buf, ss_bufused(&b->m));
	memcpy(buf->p, b->v.s, ss_bufused(&b->v));
	ss_bufadvance(buf, ss_bufused(&b->v));
	return 0;
}

int sd_writepage(sr *r, ssfile *file, sdbuild *b)
{
	SS_INJECTION(r->i, SS_INJECTION_SD_BUILD_0,
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);
	struct iovec iovv[3];
	ssiov iov;
	ss_iovinit(&iov, iovv, 3);
	if (ss_bufused(&b->c) > 0) {
		/* compressed */
		ss_iovadd(&iov, b->c.s, ss_bufused(&b->c));
	} else {
		/* uncompressed */
		ss_iovadd(&iov, b->m.s, ss_bufused(&b->m));
		ss_iovadd(&iov, b->v.s, ss_bufused(&b->v));
	}
	int rc;
	rc = ss_filewritev(file, &iov);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               ss_pathof(&file->path),
		               strerror(errno));
		return -1;
	}
	return 0;
}

int sd_writeindex(sr *r, ssfile *file, sdindex *index)
{
	int rc;
	rc = ss_filewrite(file, index->i.s, ss_bufused(&index->i));
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               ss_pathof(&file->path),
		               strerror(errno));
		return -1;
	}
	return 0;
}
