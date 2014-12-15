
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

int sd_ssinit(sdss *s)
{
	sr_bufinit(&s->buf);
	return 0;
}

int sd_ssopen(sdss *s, sr *r, srbuf *buf)
{
	sdssheader *h = (sdssheader*)buf->s;
	uint32_t crc =
		sr_crcp((char*)h + sizeof(uint32_t),
		        sr_bufused(buf) - sizeof(uint32_t), 0);
	if (srunlikely(crc != h->crc))
		return sr_error(r->e, "%s", "bad snapshot header crc");
	sr_buffree(&s->buf, r->a);
	s->buf = *buf;
	memset(buf, 0, sizeof(srbuf));
	return 0;
}

int sd_ssfree(sdss *s, sr *r)
{
	sr_buffree(&s->buf, r->a);
	return 0;
}

int sd_sscreate(sdss *s, sr *r)
{
	assert(sr_bufused(&s->buf) == 0);
	int rc = sr_bufensure(&s->buf, r->a, sizeof(sdssheader));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdssheader *h = (sdssheader*)s->buf.s;
	h->count = 0;
	h->crc = 0;
	sr_bufadvance(&s->buf, sizeof(sdssheader));
	return 0;
}

static int
sd_ssappend(sdss *s, sr *r, uint64_t lsn, char *name, int namelen)
{
	int size = sizeof(sdssrecord) + namelen;
	int rc = sr_bufensure(&s->buf, r->a, size);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdssheader *h = (sdssheader*)s->buf.s;
	sdssrecord *rp;
	rp = (sdssrecord*)s->buf.p;
	rp->lsn = lsn;
	rp->namelen = namelen;
	memcpy(rp->name, name, namelen);
	sr_bufadvance(&s->buf, size);
	h->count++;
	return 0;
}

static inline void
sd_sscrc(sdss *s)
{
	if (! sr_bufused(&s->buf))
		return;
	sdssheader *h = (sdssheader*)s->buf.s;
	uint32_t crc =
		sr_crcp((char*)h + sizeof(uint32_t),
		        sr_bufused(&s->buf) - sizeof(uint32_t), 0);
	h->crc = crc;
}

int sd_ssadd(sdss *s, sdss *n, sr *r, uint64_t lsn, char *name)
{
	int len = strlen(name);
	int rc;
	sd_ssinit(n);
	rc = sd_sscreate(n, r);
	if (srunlikely(rc == -1))
		return -1;
	sdssrecord *rp;
	sriter i;
	sr_iterinit(&i, &sd_ssiter, r);
	sr_iteropen(&i, &s->buf, 0);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		rp = sr_iterof(&i);
		if (srunlikely(strncmp(rp->name, name, len) == 0))
			goto error;
		rc = sd_ssappend(n, r, rp->lsn, rp->name, rp->namelen);
		if (srunlikely(rc == -1))
			goto error;
	}
	rc = sd_ssappend(n, r, lsn, name, len + 1);
	if (srunlikely(rc == -1))
		goto error;
	sd_sscrc(n);
	return 0;
error:
	sd_ssfree(n, r);
	return -1;
}

int sd_ssdelete(sdss *s, sdss *n, sr *r, char *name)
{
	int match = 0;
	int rc;
	sd_ssinit(n);
	rc = sd_sscreate(n, r);
	if (srunlikely(rc == -1))
		return -1;
	sriter i;
	sr_iterinit(&i, &sd_ssiter, r);
	sr_iteropen(&i, &s->buf, 0);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sdssrecord *rp = sr_iterof(&i);
		if (srunlikely(strncmp(rp->name, name, rp->namelen) == 0)) {
			match = 1;
		} else {
			rc = sd_ssappend(n, r, rp->lsn, rp->name, rp->namelen);
			if (srunlikely(rc == -1))
				goto error;
		}
	}
	if (srunlikely(! match))
		goto error;
	sd_sscrc(n);
	return 0;
error:
	sd_ssfree(n, r);
	return -1;
}
