
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
	memset(&b->tracker, 0, sizeof(b->tracker));
	ss_bufinit(&b->list);
	ss_bufinit(&b->m);
	ss_bufinit(&b->v);
	ss_bufinit(&b->c);
	ss_bufinit(&b->k);
	b->n = 0;
	b->compress = 0;
	b->compress_dup = 0;
	b->compress_if = NULL;
	b->crc = 0;
	b->vmax = 0;
}

static inline void
sd_buildfree_tracker(sdbuild *b, sr *r)
{
	if (b->tracker.count == 0)
		return;
	int i = 0;
	for (; i < b->tracker.size; i++) {
		if (b->tracker.i[i] == NULL)
			continue;
		ss_free(r->a, b->tracker.i[i]);
		b->tracker.i[i] = NULL;
	}
	b->tracker.count = 0;
}

void sd_buildfree(sdbuild *b, sr *r)
{
	sd_buildfree_tracker(b, r);
	ss_htfree(&b->tracker, r->a);
	ss_buffree(&b->list, r->a);
	ss_buffree(&b->m, r->a);
	ss_buffree(&b->v, r->a);
	ss_buffree(&b->c, r->a);
	ss_buffree(&b->k, r->a);
}

void sd_buildreset(sdbuild *b, sr *r)
{
	sd_buildfree_tracker(b, r);
	ss_htreset(&b->tracker);
	ss_bufreset(&b->list);
	ss_bufreset(&b->m);
	ss_bufreset(&b->v);
	ss_bufreset(&b->c);
	ss_bufreset(&b->k);
	b->n = 0;
	b->vmax = 0;
}

void sd_buildgc(sdbuild *b, sr *r, int wm)
{
	sd_buildfree_tracker(b, r);
	ss_htreset(&b->tracker);
	ss_bufgc(&b->list, r->a, wm);
	ss_bufgc(&b->m, r->a, wm);
	ss_bufgc(&b->v, r->a, wm);
	ss_bufgc(&b->c, r->a, wm);
	ss_bufgc(&b->k, r->a, wm);
	b->n = 0;
	b->vmax = 0;
}

int sd_buildbegin(sdbuild *b, sr *r, int crc,
                  int timestamp,
                  int compress_dup,
                  int compress,
                  ssfilterif *compress_if)
{
	b->crc = crc;
	b->compress_dup = compress_dup;
	b->compress = compress;
	b->compress_if = compress_if;
	b->timestamp = timestamp;
	int rc;
	if (compress_dup && b->tracker.size == 0) {
		rc = ss_htinit(&b->tracker, r->a, 32768);
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
	}
	rc = ss_bufensure(&b->list, r->a, sizeof(sdbuildref));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdbuildref *ref =
		(sdbuildref*)ss_bufat(&b->list, sizeof(sdbuildref), b->n);
	ref->m     = ss_bufused(&b->m);
	ref->msize = 0;
	ref->v     = ss_bufused(&b->v);
	ref->vsize = 0;
	ref->k     = ss_bufused(&b->k);
	ref->ksize = 0;
	ref->c     = ss_bufused(&b->c);
	ref->csize = 0;
	rc = ss_bufensure(&b->m, r->a, sizeof(sdpageheader));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdpageheader *h = sd_buildheader(b);
	memset(h, 0, sizeof(*h));
	h->lsnmin    = UINT64_MAX;
	h->lsnmindup = UINT64_MAX;
	h->tsmin     = UINT32_MAX;
	h->reserve   = 0;
	ss_bufadvance(&b->list, sizeof(sdbuildref));
	ss_bufadvance(&b->m, sizeof(sdpageheader));
	return 0;
}

typedef struct {
	sshtnode node;
	uint32_t offset;
	uint32_t offsetstart;
	uint32_t size;
} sdbuildkey;

ss_htsearch(sd_buildsearch,
            (sscast(t->i[pos], sdbuildkey, node)->node.hash == hash) &&
            (sscast(t->i[pos], sdbuildkey, node)->size == size) &&
            (memcmp(((sdbuild*)ptr)->k.s +
                    sscast(t->i[pos], sdbuildkey, node)->offsetstart, key, size) == 0))

static inline int
sd_buildadd_keyvalue(sdbuild *b, sr *r, sv *v,
                     uint32_t timestamp,
                     uint64_t lsn)
{
	/* calculate key size */
	uint32_t keysize = 0;
	int i = 0;
	while (i < r->scheme->count) {
		keysize += sv_keysize(v, r, i);
		i++;
	}
	uint32_t valuesize = sv_valuesize(v, r);
	uint32_t size = keysize + valuesize;

	/* prepare buffer */
	uint32_t sizemeta = ss_leb128size(size) + ss_leb128size(lsn);
	if (b->timestamp)
		sizemeta += ss_leb128size(timestamp);
	int rc = ss_bufensure(&b->v, r->a, sizemeta);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);

	/* write meta */
	ss_bufadvance(&b->v, ss_leb128write(b->v.p, size));
	ss_bufadvance(&b->v, ss_leb128write(b->v.p, lsn));
	if (b->timestamp)
		ss_bufadvance(&b->v, ss_leb128write(b->v.p, timestamp));

	/* write key-parts */
	i = 0;
	for (; i < r->scheme->count; i++)
	{
		uint32_t partsize = sv_keysize(v, r, i);
		char *part = sv_key(v, r, i);

		int offsetstart = ss_bufused(&b->k);
		int offset = (offsetstart - sd_buildref(b)->k);

		/* match a key copy */
		int is_duplicate = 0;
		uint32_t hash = 0;
		int pos = 0;
		if (b->compress_dup) {
			hash = ss_fnv(part, partsize);
			pos = sd_buildsearch(&b->tracker, hash, part, partsize, b);
			if (b->tracker.i[pos]) {
				is_duplicate = 1;
				sdbuildkey *ref = sscast(b->tracker.i[pos], sdbuildkey, node);
				offset = ref->offset;
			}
		}

		/* offset */
		rc = ss_bufensure(&b->v, r->a, ss_leb128size(offset));
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
		ss_bufadvance(&b->v, ss_leb128write(b->v.p, offset));
		if (is_duplicate)
			continue;

		/* copy key */
		int partsize_meta = ss_leb128size(partsize);
		rc = ss_bufensure(&b->k, r->a, partsize_meta + partsize);
		if (ssunlikely(rc == -1))
			return sr_oom(r->e);
		ss_bufadvance(&b->k, ss_leb128write(b->k.p, partsize));
		memcpy(b->k.p, part, partsize);
		ss_bufadvance(&b->k, partsize);

		/* add key reference */
		if (b->compress_dup) {
			if (ssunlikely(ss_htisfull(&b->tracker))) {
				rc = ss_htresize(&b->tracker, r->a);
				if (ssunlikely(rc == -1))
					return sr_oom(r->e);
			}
			sdbuildkey *ref = ss_malloc(r->a, sizeof(sdbuildkey));
			if (ssunlikely(ref == NULL))
				return sr_oom(r->e);
			ref->node.hash = hash;
			ref->offset = offset;
			ref->offsetstart = offsetstart + partsize_meta;
			ref->size = partsize;
			ss_htset(&b->tracker, pos, &ref->node);
		}
	}

	/* write value */
	rc = ss_bufensure(&b->v, r->a, valuesize);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	memcpy(b->v.p, sv_value(v, r), valuesize);
	ss_bufadvance(&b->v, valuesize);
	return 0;
}

static inline int
sd_buildadd_raw(sdbuild *b, sr *r, sv *v,
                uint32_t size,
                uint32_t timestamp,
                uint64_t lsn)
{
	uint32_t sizemeta = ss_leb128size(size) + ss_leb128size(lsn);
	if (b->timestamp)
		sizemeta += ss_leb128size(timestamp);
	int rc = ss_bufensure(&b->v, r->a, sizemeta + size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	ss_bufadvance(&b->v, ss_leb128write(b->v.p, size));
	ss_bufadvance(&b->v, ss_leb128write(b->v.p, lsn));
	if (b->timestamp)
		ss_bufadvance(&b->v, ss_leb128write(b->v.p, timestamp));
	memcpy(b->v.p, sv_pointer(v), size);
	ss_bufadvance(&b->v, size);
	return 0;
}

int sd_buildadd(sdbuild *b, sr *r, sv *v, uint32_t flags)
{
	/* prepare document metadata */
	int rc = ss_bufensure(&b->m, r->a, sizeof(sdv));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	uint64_t lsn = sv_lsn(v);
	uint32_t timestamp = sv_timestamp(v);
	uint32_t size = sv_size(v);
	sdpageheader *h = sd_buildheader(b);
	sdv *sv = (sdv*)b->m.p;
	sv->flags = flags;
	if (b->timestamp)
		sv->flags |= SVTIMESTAMP;
	sv->offset = ss_bufused(&b->v) - sd_buildref(b)->v;
	ss_bufadvance(&b->m, sizeof(sdv));
	/* copy document */
	switch (r->fmt_storage) {
	case SF_SKEYVALUE:
		rc = sd_buildadd_keyvalue(b, r, v, timestamp, lsn);
		break;
	case SF_SRAW:
		rc = sd_buildadd_raw(b, r, v, size, timestamp, lsn);
		break;
	}
	if (ssunlikely(rc == -1))
		return -1;
	/* update page header */
	h->count++;
	size += sizeof(sdv) + 20 + sizeof(sfref) * r->scheme->count;
	if (size > b->vmax)
		b->vmax = size;
	if (lsn > h->lsnmax)
		h->lsnmax = lsn;
	if (lsn < h->lsnmin)
		h->lsnmin = lsn;
	if (timestamp < h->tsmin)
		h->tsmin = timestamp;
	if (sv->flags & SVDUP) {
		h->countdup++;
		if (lsn < h->lsnmindup)
			h->lsnmindup = lsn;
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
	sdbuildref *ref = sd_buildref(b);
	ssfilter f;
	rc = ss_filterinit(&f, b->compress_if, r->a, SS_FINPUT);
	if (ssunlikely(rc == -1))
		return -1;
	rc = ss_filterstart(&f, &b->c);
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filternext(&f, &b->c, b->m.s + ref->m + sizeof(sdpageheader),
	                   ref->msize - sizeof(sdpageheader));
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filternext(&f, &b->c, b->v.s + ref->v, ref->vsize);
	if (ssunlikely(rc == -1))
		goto error;
	rc = ss_filternext(&f, &b->c, b->k.s + ref->k, ref->ksize);
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
	/* update sizes */
	sdbuildref *ref = sd_buildref(b);
	ref->msize = ss_bufused(&b->m) - ref->m;
	ref->vsize = ss_bufused(&b->v) - ref->v;
	ref->ksize = ss_bufused(&b->k) - ref->k;
	ref->csize = 0;
	/* calculate data crc (non-compressed) */
	sdpageheader *h = sd_buildheader(b);
	uint32_t crc = 0;
	if (sslikely(b->crc)) {
		crc = ss_crcp(r->crc, b->m.s + ref->m, ref->msize, 0);
		crc = ss_crcp(r->crc, b->v.s + ref->v, ref->vsize, crc);
		crc = ss_crcp(r->crc, b->k.s + ref->k, ref->ksize, crc);
	}
	h->crcdata = crc;
	/* compression */
	if (b->compress) {
		int rc = sd_buildcompress(b, r);
		if (ssunlikely(rc == -1))
			return -1;
		ref->csize = ss_bufused(&b->c) - ref->c;
	}
	/* update page header */
	int total = ref->msize + ref->vsize + ref->ksize;
	h->sizekeys = ref->ksize;
	h->sizeorigin = total - sizeof(sdpageheader);
	h->size = h->sizeorigin;
	if (b->compress)
		h->size = ref->csize - sizeof(sdpageheader);
	else
		h->size = h->sizeorigin;
	h->crc = ss_crcs(r->crc, h, sizeof(sdpageheader), 0);
	if (b->compress)
		memcpy(b->c.s + ref->c, h, sizeof(sdpageheader));
	return 0;
}

int sd_buildcommit(sdbuild *b, sr *r)
{
	if (b->compress_dup)
		sd_buildfree_tracker(b, r);
	if (b->compress) {
		ss_bufreset(&b->m);
		ss_bufreset(&b->v);
		ss_bufreset(&b->k);
	}
	b->n++;
	return 0;
}
