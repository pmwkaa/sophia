
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

void sd_buildinit(sdbuild *b)
{
	ss_bufinit(&b->m);
	ss_bufinit(&b->v);
	ss_bufinit(&b->c);
	b->compress = 0;
	b->compress_if = NULL;
	b->crc = 0;
	b->vmax = 0;
}

void sd_buildfree(sdbuild *b, sr *r)
{
	ss_buffree(&b->m, r->a);
	ss_buffree(&b->v, r->a);
	ss_buffree(&b->c, r->a);
}

void sd_buildreset(sdbuild *b)
{
	ss_bufreset(&b->m);
	ss_bufreset(&b->v);
	ss_bufreset(&b->c);
	b->vmax = 0;
}

void sd_buildgc(sdbuild *b, sr *r, int wm)
{
	ss_bufgc(&b->m, r->a, wm);
	ss_bufgc(&b->v, r->a, wm);
	ss_bufgc(&b->c, r->a, wm);
	b->vmax = 0;
}

int sd_buildbegin(sdbuild *b, sr *r, int crc,
                  int compress,
                  ssfilterif *compress_if)
{
	b->crc = crc;
	b->compress = compress;
	b->compress_if = compress_if;
	int rc;
	rc = ss_bufensure(&b->m, r->a, sizeof(sdpageheader));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdpageheader *h = sd_buildheader(b);
	memset(h, 0, sizeof(*h));
	h->lsnmin    = UINT64_MAX;
	h->lsnmindup = UINT64_MAX;
	h->tsmin     = UINT32_MAX;
	h->reserve   = 0;
	ss_bufadvance(&b->m, sizeof(sdpageheader));
	return 0;
}

static inline int
sd_buildadd_raw(sdbuild *b, sr *r, char *v, uint8_t flags)
{
	uint32_t size = sf_size(r->scheme, v);
	int rc = ss_bufensure(&b->v, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	memcpy(b->v.p, v, size);
	sf_flagsset(r->scheme, b->v.p, flags);
	ss_bufadvance(&b->v, size);
	return 0;
}

int sd_buildadd(sdbuild *b, sr *r, char *v, uint8_t flags)
{
	uint32_t size = sf_size(r->scheme, v);

	/* store document offset */
	int rc;
	if (! sf_schemefixed(r->scheme)) {
		uint32_t offset = ss_bufused(&b->v);
		rc = ss_bufadd(&b->m, r->a, &offset, sizeof(offset));
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
		size += sizeof(offset);
	}

	/* copy document */
	rc = sd_buildadd_raw(b, r, v, flags);
	if (ssunlikely(rc == -1))
		return -1;

	/* update page header */
	sdpageheader *h = sd_buildheader(b);
	h->count++;
	if (size > b->vmax)
		b->vmax = size;
	uint64_t lsn = sf_lsn(r->scheme, v);
	if (lsn > h->lsnmax)
		h->lsnmax = lsn;
	if (lsn < h->lsnmin)
		h->lsnmin = lsn;
	if (flags & SVDUP) {
		h->countdup++;
		if (lsn < h->lsnmindup)
			h->lsnmindup = lsn;
	}
	if (r->scheme->has_expire) {
		uint32_t timestamp = sf_ttl(r->scheme, v);
		if (timestamp < h->tsmin)
			h->tsmin = timestamp;
	}
	return 0;
}

static inline int
sd_buildcompress(sdbuild *b, sr *r)
{
	assert(b->compress_if != &ss_nonefilter);
	/* reserve header */
	int rc = ss_bufensure(&b->c, r->a, sizeof(sdpageheader));
	if (ssunlikely(rc == -1))
		return -1;
	ss_bufadvance(&b->c, sizeof(sdpageheader));
	/* compression (including meta-data) */
	ssfilter f;
	rc = ss_filterinit(&f, b->compress_if, r->a, SS_FINPUT);
	if (ssunlikely(rc == -1))
		return -1;
	rc = ss_filterstart(&f, &b->c);
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filternext(&f, &b->c, b->m.s + sizeof(sdpageheader),
	                   ss_bufused(&b->m) - sizeof(sdpageheader));
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filternext(&f, &b->c, b->v.s, ss_bufused(&b->v));
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filtercomplete(&f, &b->c);
	if (ssunlikely(rc == -1))
		goto error;
	ss_filterfree(&f);
	return 0;
error:
	ss_filterfree(&f);
	return -1;
}

int sd_buildend(sdbuild *b, sr *r)
{
	/* calculate data crc (non-compressed) */
	sdpageheader *h = sd_buildheader(b);
	uint32_t crc = 0;
	if (sslikely(b->crc)) {
		crc = ss_crcp(r->crc, b->m.s, ss_bufused(&b->m), 0);
		crc = ss_crcp(r->crc, b->v.s, ss_bufused(&b->v), crc);
	}
	h->crcdata = crc;
	/* compression */
	if (b->compress) {
		int rc = sd_buildcompress(b, r);
		if (ssunlikely(rc == -1))
			return -1;
	}
	/* update page header */
	int total = ss_bufused(&b->m) + ss_bufused(&b->v);
	h->sizeorigin = total - sizeof(sdpageheader);
	h->size = h->sizeorigin;
	if (b->compress)
		h->size = ss_bufused(&b->c) - sizeof(sdpageheader);
	else
		h->size = h->sizeorigin;
	h->crc = ss_crcs(r->crc, h, sizeof(sdpageheader), 0);
	if (b->compress)
		memcpy(b->c.s, h, sizeof(sdpageheader));
	return 0;
}
