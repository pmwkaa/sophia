
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

int sd_ioinit(sdio *s)
{
	ss_bufinit(&s->buf);
	s->size_page  = 0;
	s->size_align = 0;
	s->direct     = 0;
	return 0;
}

int sd_ioprepare(sdio *s, sr *r,
                 int direct,
                 uint32_t size_page,
                 uint32_t size_buffer)
{
	if (ssunlikely(s->buf.s))
		return 0;
	s->direct = direct;
	s->size_page = size_page;
	ss_bufinit(&s->buf);
	int rc = ss_bufensure(&s->buf, r->a, size_buffer);
	if (ssunlikely(rc == -1))
		return -1;
	char *start =
		(char*)((((intptr_t)s->buf.s + s->size_page - 1) / s->size_page) *
		        s->size_page);
	s->size_align = start - s->buf.s;
	ss_bufadvance(&s->buf, s->size_align);
	return 0;
}

int sd_iofree(sdio *s, sr *r)
{
	ss_buffree(&s->buf, r->a);
	return 0;
}

int sd_ioreset(sdio *s)
{
	ss_bufreset(&s->buf);
	ss_bufadvance(&s->buf, s->size_align);
	return 0;
}

static inline int
sd_ioflush_direct(sdio *s, sr *r, ssfile *f)
{
	/* write full pages only */
	uint32_t total = ss_bufused(&s->buf) - s->size_align;
	uint32_t count = total / s->size_page;
	if (count == 0)
		return 0;
	uint32_t size  = count * s->size_page;
	int rc = ss_filewrite(f, s->buf.s + s->size_align, size);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               ss_pathof(&f->path),
		               strerror(errno));
		return -1;
	}
	/* copy unaligned part to the beginning
	 * of the buffer */
	uint32_t leftover = total % s->size_page;
	s->buf.p = s->buf.s + s->size_align;
	if (leftover)
		memmove(s->buf.p, s->buf.s + s->size_align + size, leftover);
	ss_bufadvance(&s->buf, leftover);
	return 0;
}

int sd_ioflush(sdio *s, sr *r, ssfile *f)
{
	if (! s->direct) {
		/* unused */
		return 0;
	}
	return sd_ioflush_direct(s, r, f);
}

static inline int
sd_iowrite_direct(sdio *s, sr *r, ssfile *f, char *buf, int size)
{
	int rc;
	if ((ss_bufused(&s->buf) + size) > ss_bufsize(&s->buf)) {
		rc = sd_ioflush(s, r, f);
		if (ssunlikely(rc == -1))
			return -1;
	}
	rc = ss_bufadd(&s->buf, r->a, buf, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	return 0;
}

int sd_iowrite(sdio *s, sr *r, ssfile *f, char *buf, int size)
{
	if (s->direct)
		return sd_iowrite_direct(s, r, f, buf, size);
	int rc;
	rc = ss_filewrite(f, buf, size);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "file '%s' write error: %s",
		               ss_pathof(&f->path),
		               strerror(errno));
		return -1;
	}
	return 0;
}

static inline int
sd_ioread_direct(sdio *s, sr *r, ssfile *f, uint64_t offset,
                 char *buf, int size, int from_compaction,
                 char **buf_align)
{
	uint32_t page_size = s->size_page;

	/* align buffer */
	char *buf_aligned =
		(char*)((((intptr_t)buf + page_size - 1) / page_size) * page_size);

	/* calculate aligned offset and size */
	uint64_t offset_align = offset % page_size;
	offset -= offset_align;
	size   += offset_align;
	uint64_t size_align   = size % page_size;
	if (size_align > 0) {
		size += page_size - size % page_size;
	}

	/* read */
	uint64_t start = ss_utime();
	int rc;
	rc = ss_filepread(f, offset, buf_aligned, size);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
		         ss_pathof(&f->path),
		         strerror(errno));
		return -1;
	}
	sr_statpread(r->stat, start, from_compaction);
	*buf_align = buf_aligned + offset_align;
	return 0;
}

int sd_ioread(sdio *s, sr *r, ssfile *f, uint64_t offset,
              char *buf, int size, int from_compaction,
              char **buf_align)
{
	if (s->direct)
		return sd_ioread_direct(s, r, f, offset, buf, size,
		                        from_compaction,
		                        buf_align);
	uint64_t start = ss_utime();
	int rc;
	rc = ss_filepread(f, offset, buf, size);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' read error: %s",
		         ss_pathof(&f->path),
		         strerror(errno));
		return -1;
	}
	sr_statpread(r->stat, start, from_compaction);
	*buf_align = buf;
	return 0;
}
