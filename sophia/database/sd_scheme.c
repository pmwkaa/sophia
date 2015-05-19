
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

int sd_schemebegin(sdscheme *c, sr *r)
{
	int rc = sr_bufensure(&c->buf, r->a, sizeof(sdschemeheader));
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	sdschemeheader *h = (sdschemeheader*)c->buf.s;
	memset(h, 0, sizeof(sdschemeheader));
	sr_bufadvance(&c->buf, sizeof(sdschemeheader));
	return 0;
}

int sd_schemeadd(sdscheme *c, sr *r, uint8_t id, srtype type,
                 void *value, uint32_t size)
{
	sdschemeopt opt = {
		.type = type,
		.id   = id,
		.size = size
	};
	int rc = sr_bufadd(&c->buf, r->a, &opt, sizeof(opt));
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_bufadd(&c->buf, r->a, value, size);
	if (srunlikely(rc == -1))
		goto error;
	sdschemeheader *h = (sdschemeheader*)c->buf.s;
	h->count++;
	return 0;
error:
	sr_error(r->e, "%s", "memory allocation failed");
	return -1;
}

int sd_schemecommit(sdscheme *c, sr *r)
{
	if (srunlikely(sr_bufused(&c->buf) == 0))
		return 0;
	sdschemeheader *h = (sdschemeheader*)c->buf.s;
	h->size = sr_bufused(&c->buf) - sizeof(sdschemeheader);
	h->crc  = sr_crcs(r->crc, (char*)h, sr_bufused(&c->buf), 0);
	return 0;
}

int sd_schemewrite(sdscheme *c, sr *r, char *path, int sync)
{
	srfile meta;
	sr_fileinit(&meta, r->a);
	int rc = sr_filenew(&meta, path);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filewrite(&meta, c->buf.s, sr_bufused(&c->buf));
	if (srunlikely(rc == -1))
		goto error;
	if (sync) {
		rc = sr_filesync(&meta);
		if (srunlikely(rc == -1))
			goto error;
	}
	rc = sr_fileclose(&meta);
	if (srunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_error(r->e, "%s", "scheme file '%s' error: %s",
	         path, strerror(errno));
	sr_fileclose(&meta);
	return -1;
}

int sd_schemerecover(sdscheme *c, sr *r, char *path)
{
	ssize_t size = sr_filesize(path);
	if (srunlikely(size == -1))
		goto error;
	if (srunlikely((unsigned int)size < sizeof(sdschemeheader))) {
		sr_error(r->e, "%s", "scheme file '%s' is corrupted", path);
		return -1;
	}
	int rc = sr_bufensure(&c->buf, r->a, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	srfile meta;
	sr_fileinit(&meta, r->a);
	rc = sr_fileopen(&meta, path);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filepread(&meta, 0, c->buf.s, size);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_fileclose(&meta);
	if (srunlikely(rc == -1))
		goto error;
	sr_bufadvance(&c->buf, size);
	return 0;
error:
	sr_error(r->e, "%s", "scheme file '%s' error: %s",
	         path, strerror(errno));
	return -1;
}
