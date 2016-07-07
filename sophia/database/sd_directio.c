
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

int sd_directio_init(sddirectio *s)
{
	ss_bufinit(&s->buf);
	s->size_page  = 0;
	s->size_align = 0;
	return 0;
}

int sd_directio_prepare(sddirectio *s, sr *r,
                        uint32_t size_page,
                        uint32_t size_buffer)
{
	if (ssunlikely(s->buf.s))
		return 0;
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

int sd_directio_free(sddirectio *s, sr *r)
{
	ss_buffree(&s->buf, r->a);
	return 0;
}

int sd_directio_reset(sddirectio *s)
{
	ss_bufreset(&s->buf);
	ss_bufadvance(&s->buf, s->size_align);
	return 0;
}

int sd_directio_write(sddirectio *s, ssfile *f, sr *r, char *buf, int size)
{
	int rc;
	if ((ss_bufused(&s->buf) + size) > ss_bufsize(&s->buf)) {
		rc = sd_directio_flush(s, f);
		if (ssunlikely(rc == -1))
			return -1;
	}
	rc = ss_bufadd(&s->buf, r->a, buf, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	return 0;
}

int sd_directio_flush(sddirectio *s, ssfile *f)
{
	/* write full pages only */
	uint32_t total = ss_bufused(&s->buf) - s->size_align;
	uint32_t count = total / s->size_page;
	if (count == 0)
		return 0;
	uint32_t size  = count * s->size_page;
	int rc = ss_filewrite(f, s->buf.s + s->size_align, size);
	if (ssunlikely(rc == -1)) {
		printf("stream flush error: %d\n", errno);
		fflush(NULL);
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
