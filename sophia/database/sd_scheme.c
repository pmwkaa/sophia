
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

int sd_schemebegin(sdscheme *c, sr *r)
{
	int rc = ss_bufensure(&c->buf, r->a, sizeof(sdschemeheader));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdschemeheader *h = (sdschemeheader*)c->buf.s;
	memset(h, 0, sizeof(sdschemeheader));
	ss_bufadvance(&c->buf, sizeof(sdschemeheader));
	return 0;
}

int sd_schemeadd(sdscheme *c, sr *r, uint8_t id, sstype type,
                 void *value, uint32_t size)
{
	sdschemeopt opt = {
		.type = type,
		.id   = id,
		.size = size
	};
	int rc = ss_bufadd(&c->buf, r->a, &opt, sizeof(opt));
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_bufadd(&c->buf, r->a, value, size);
	if (ssunlikely(rc == -1))
		goto error;
	sdschemeheader *h = (sdschemeheader*)c->buf.s;
	h->count++;
	return 0;
error:
	return sr_oom(r->e);
}

int sd_schemecommit(sdscheme *c, sr *r)
{
	if (ssunlikely(ss_bufused(&c->buf) == 0))
		return 0;
	sdschemeheader *h = (sdschemeheader*)c->buf.s;
	h->size = ss_bufused(&c->buf) - sizeof(sdschemeheader);
	h->crc  = ss_crcs(r->crc, (char*)h, ss_bufused(&c->buf), 0);
	return 0;
}

int sd_schemewrite(sdscheme *c, sr *r, char *path, int sync)
{
	ssfile meta;
	ss_fileinit(&meta, r->vfs);
	int rc = ss_filenew(&meta, path, 0);
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filewrite(&meta, c->buf.s, ss_bufused(&c->buf));
	if (ssunlikely(rc == -1))
		goto error;
	if (sync) {
		rc = ss_filesync(&meta);
		if (ssunlikely(rc == -1))
			goto error;
	}
	ss_fileadvise(&meta, 0, 0, meta.size);
	rc = ss_fileclose(&meta);
	if (ssunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_error(r->e, "scheme file '%s' error: %s",
	         path, strerror(errno));
	ss_fileclose(&meta);
	return -1;
}

int sd_schemerecover(sdscheme *c, sr *r, char *path)
{
	ssize_t size = ss_vfssize(r->vfs, path);
	if (ssunlikely(size == -1))
		goto error;
	if (ssunlikely((unsigned int)size < sizeof(sdschemeheader))) {
		sr_error(r->e, "scheme file '%s' is corrupted", path);
		return -1;
	}
	int rc = ss_bufensure(&c->buf, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	ssfile meta;
	ss_fileinit(&meta, r->vfs);
	rc = ss_fileopen(&meta, path, 0);
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filepread(&meta, 0, c->buf.s, size);
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_fileclose(&meta);
	if (ssunlikely(rc == -1))
		goto error;
	ss_bufadvance(&c->buf, size);
	return 0;
error:
	sr_error(r->e, "scheme file '%s' error: %s",
	         path, strerror(errno));
	return -1;
}
