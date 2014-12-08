
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

int sd_ssinit(sdss *s, sr *r)
{
	sr_bufinit(&s->buf);
	int rc = sr_bufensure(&s->buf, r->a, sizeof(sdssheader));
	if (srunlikely(rc == -1))
		return -1;
	sdssheader *h = (sdssheader*)s->buf.s;
	h->crc = 0;
	h->count = 0;
	sr_bufadvance(&s->buf, sizeof(sdssheader));
	return 0;
}

int sd_ssset(sdss *s, sr *r, srbuf *buf)
{
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

static int
sd_ssappend(sdss *s, sr *r, uint64_t lsn, char *name, int namelen)
{
	int size = sizeof(sdssrecord) + namelen;
	int rc = sr_bufensure(&s->buf, r->a, size);
	if (srunlikely(rc == -1))
		return -1;
	sdssrecord *rp;
	rp = (sdssrecord*)s->buf.p;
	rp->lsn = lsn;
	rp->namelen = namelen;
	memcpy(rp->name, name, namelen);
	sr_bufadvance(&s->buf, size);
	sdssheader *h = (sdssheader*)s->buf.s;
	h->count++;
	h->crc = sr_crcs(h, sizeof(sdssheader), 0);
	return 0;
}

int sd_ssadd(sdss *s, sr *r, uint64_t lsn, char *name)
{
	int len = strlen(name);
	sdssrecord *rp;
	sriter i;
	sr_iterinit(&i, &sd_ssiter, r);
	sr_iteropen(&i, &s->buf, 0);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		rp = sr_iterof(&i);
		if (srunlikely(strncmp(rp->name, name, len) == 0))
			return -1;
	}
	return sd_ssappend(s, r, lsn, name, len + 1);
}

int sd_ssdelete(sdss *s, sr *r, char *name)
{
	sdss n;
	int rc = sd_ssinit(&n, r);
	if (srunlikely(rc == -1))
		return -1;
	int match = 0;
	sriter i;
	sr_iterinit(&i, &sd_ssiter, r);
	sr_iteropen(&i, &s->buf, 0);
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sdssrecord *rp = sr_iterof(&i);
		if (srunlikely(strncmp(rp->name, name, rp->namelen) == 0)) {
			match = 1;
		} else {
			rc = sd_ssappend(&n, r, rp->lsn, rp->name, rp->namelen);
			if (srunlikely(rc == -1)) {
				sd_ssfree(&n, r);
				return -1;
			}
		}
	}
	if (srunlikely(! match)) {
		sd_ssfree(&n, r);
		return -1;
	}
	sd_ssfree(s, r);
	*s = n;
	return 0;
}
