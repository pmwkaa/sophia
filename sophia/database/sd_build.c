
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

void sd_buildinit(sdbuild *b)
{
	memset(&b->tracker, 0, sizeof(b->tracker));
	sr_bufinit(&b->list);
	sr_bufinit(&b->m);
	sr_bufinit(&b->v);
	sr_bufinit(&b->c);
	sr_bufinit(&b->k);
	b->n = 0;
	b->compress = 0;
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
		sr_free(r->a, b->tracker.i[i]);
		b->tracker.i[i] = NULL;
	}
	b->tracker.count = 0;
}

void sd_buildfree(sdbuild *b, sr *r)
{
	sd_buildfree_tracker(b, r);
	sr_htfree(&b->tracker, r->a);
	sr_buffree(&b->list, r->a);
	sr_buffree(&b->m, r->a);
	sr_buffree(&b->v, r->a);
	sr_buffree(&b->c, r->a);
	sr_buffree(&b->k, r->a);
}

void sd_buildreset(sdbuild *b)
{
	sr_htreset(&b->tracker);
	sr_bufreset(&b->list);
	sr_bufreset(&b->m);
	sr_bufreset(&b->v);
	sr_bufreset(&b->c);
	sr_bufreset(&b->k);
	b->n = 0;
	b->vmax = 0;
}

int sd_buildbegin(sdbuild *b, sr *r, int crc, int compress, int compress_dup)
{
	b->crc = crc;
	b->compress = compress;
	b->compress_dup = compress_dup;
	int rc;
	if (compress_dup && b->tracker.size == 0) {
		rc = sr_htinit(&b->tracker, r->a, 32768);
		if (srunlikely(rc == -1))
			return sr_error(r->e, "%s", "memory allocation failed");
	}
	rc = sr_bufensure(&b->list, r->a, sizeof(sdbuildref));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdbuildref *ref =
		(sdbuildref*)sr_bufat(&b->list, sizeof(sdbuildref), b->n);
	ref->m     = sr_bufused(&b->m);
	ref->msize = 0;
	ref->v     = sr_bufused(&b->v);
	ref->vsize = 0;
	ref->k     = sr_bufused(&b->k);
	ref->ksize = 0;
	ref->c     = sr_bufused(&b->c);
	ref->csize = 0;
	rc = sr_bufensure(&b->m, r->a, sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdpageheader *h = sd_buildheader(b);
	memset(h, 0, sizeof(*h));
	h->lsnmin    = UINT64_MAX;
	h->lsnmindup = UINT64_MAX;
	memset(h->reserve, 0, sizeof(h->reserve));
	sr_bufadvance(&b->list, sizeof(sdbuildref));
	sr_bufadvance(&b->m, sizeof(sdpageheader));
	return 0;
}

typedef struct {
	srhtnode node;
	uint32_t offset;
	uint32_t offsetstart;
	uint32_t size;
} sdbuildkey;

sr_htsearch(sd_buildsearch,
            (srcast(t->i[pos], sdbuildkey, node)->node.hash == hash) &&
            (srcast(t->i[pos], sdbuildkey, node)->size == size) &&
            (memcmp(((sdbuild*)ptr)->k.s +
                    srcast(t->i[pos], sdbuildkey, node)->offsetstart, key, size) == 0))

static inline int
sd_buildadd_keyvalue(sdbuild *b, sr *r, sv *v)
{
	/* calculate key size */
	uint32_t keysize = 0;
	int i = 0;
	while (i < r->cmp->count) {
		keysize += sv_keysize(v, r, i);
		i++;
	}
	uint32_t valuesize = sv_valuesize(v, r);
	uint32_t size = keysize + valuesize;

	/* prepare buffer */
	uint64_t lsn = sv_lsn(v);
	uint32_t sizemeta = sr_leb128size(size) + sr_leb128size(lsn);
	int rc = sr_bufensure(&b->v, r->a, sizemeta);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");

	/* write meta */
	sr_bufadvance(&b->v, sr_leb128write(b->v.p, size));
	sr_bufadvance(&b->v, sr_leb128write(b->v.p, lsn));

	/* write key-parts */
	i = 0;
	for (; i < r->cmp->count; i++)
	{
		uint32_t partsize = sv_keysize(v, r, i);
		char *part = sv_key(v, r, i);

		int offsetstart = sr_bufused(&b->k);
		int offset = (offsetstart - sd_buildref(b)->k);

		/* match a key copy */
		int is_duplicate = 0;
		uint32_t hash = 0;
		int pos = 0;
		if (b->compress_dup) {
			hash = sr_fnv(part, partsize);
			pos = sd_buildsearch(&b->tracker, hash, part, partsize, b);
			if (b->tracker.i[pos]) {
				is_duplicate = 1;
				sdbuildkey *ref = srcast(b->tracker.i[pos], sdbuildkey, node);
				offset = ref->offset;
			}
		}

		/* offset */
		rc = sr_bufensure(&b->v, r->a, sr_leb128size(offset));
		if (srunlikely(rc == -1))
			return sr_error(r->e, "%s", "memory allocation failed");
		sr_bufadvance(&b->v, sr_leb128write(b->v.p, offset));
		if (is_duplicate)
			continue;

		/* copy key */
		int partsize_meta = sr_leb128size(partsize);
		rc = sr_bufensure(&b->k, r->a, partsize_meta + partsize);
		if (srunlikely(rc == -1))
			return sr_error(r->e, "%s", "memory allocation failed");
		sr_bufadvance(&b->k, sr_leb128write(b->k.p, partsize));
		memcpy(b->k.p, part, partsize);
		sr_bufadvance(&b->k, partsize);

		/* add key reference */
		if (b->compress_dup) {
			if (srunlikely(sr_htisfull(&b->tracker))) {
				rc = sr_htresize(&b->tracker, r->a);
				if (srunlikely(rc == -1))
					return sr_error(r->e, "%s", "memory allocation failed");
			}
			sdbuildkey *ref = sr_malloc(r->a, sizeof(sdbuildkey));
			if (srunlikely(rc == -1))
				return sr_error(r->e, "%s", "memory allocation failed");
			ref->node.hash = hash;
			ref->offset = offset;
			ref->offsetstart = offsetstart + partsize_meta;
			ref->size = partsize;
			sr_htset(&b->tracker, pos, &ref->node);
		}
	}

	/* write value */
	rc = sr_bufensure(&b->v, r->a, valuesize);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	memcpy(b->v.p, sv_value(v, r), valuesize);
	sr_bufadvance(&b->v, valuesize);
	return 0;
}

static inline int
sd_buildadd_raw(sdbuild *b, sr *r, sv *v)
{
	uint64_t lsn = sv_lsn(v);
	uint32_t size = sv_size(v);
	uint32_t sizemeta = sr_leb128size(size) + sr_leb128size(lsn);
	int rc = sr_bufensure(&b->v, r->a, sizemeta + size);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sr_bufadvance(&b->v, sr_leb128write(b->v.p, size));
	sr_bufadvance(&b->v, sr_leb128write(b->v.p, lsn));
	memcpy(b->v.p, sv_pointer(v), size);
	sr_bufadvance(&b->v, size);
	return 0;
}

int sd_buildadd(sdbuild *b, sr *r, sv *v, uint32_t flags)
{
	/* prepare object metadata */
	int rc = sr_bufensure(&b->m, r->a, sizeof(sdv));
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sdpageheader *h = sd_buildheader(b);
	sdv *sv = (sdv*)b->m.p;
	sv->flags  = sv_flags(v) | flags;
	sv->offset = sr_bufused(&b->v) - sd_buildref(b)->v;
	sr_bufadvance(&b->m, sizeof(sdv));
	/* copy object */
	switch (r->fmt_storage) {
	case SR_FS_KEYVALUE:
		rc = sd_buildadd_keyvalue(b, r, v);
		break;
	case SR_FS_RAW:
		rc = sd_buildadd_raw(b, r, v);
		break;
	}
	if (srunlikely(rc == -1))
		return -1;
	/* update page header */
	h->count++;
	uint32_t size = sizeof(sdv) + sv_size(v) +
		sizeof(srfmtref) * r->cmp->count;
	if (size > b->vmax)
		b->vmax = size;
	uint64_t lsn = sv_lsn(v);
	if (lsn > h->lsnmax)
		h->lsnmax = lsn;
	if (lsn < h->lsnmin)
		h->lsnmin = lsn;
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
	/* reserve header */
	int rc = sr_bufensure(&b->c, r->a, sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		return -1;
	sr_bufadvance(&b->c, sizeof(sdpageheader));
	/* compression (including meta-data) */
	sdbuildref *ref = sd_buildref(b);
	srfilter f;
	rc = sr_filterinit(&f, (srfilterif*)r->compression, r, SR_FINPUT);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_filterstart(&f, &b->c);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filternext(&f, &b->c, b->m.s + ref->m + sizeof(sdpageheader),
	                   ref->msize - sizeof(sdpageheader));
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filternext(&f, &b->c, b->v.s + ref->v, ref->vsize);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filternext(&f, &b->c, b->k.s + ref->k, ref->ksize);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_filtercomplete(&f, &b->c);
	if (srunlikely(rc == -1))
		goto error;
	sr_filterfree(&f);
	return 0;
error:
	sr_filterfree(&f);
	return -1;
}

int sd_buildend(sdbuild *b, sr *r)
{
	/* update sizes */
	sdbuildref *ref = sd_buildref(b);
	ref->msize = sr_bufused(&b->m) - ref->m;
	ref->vsize = sr_bufused(&b->v) - ref->v;
	ref->ksize = sr_bufused(&b->k) - ref->k;
	ref->csize = 0;
	/* calculate data crc (non-compressed) */
	sdpageheader *h = sd_buildheader(b);
	uint32_t crc = 0;
	if (srlikely(b->crc)) {
		crc = sr_crcp(r->crc, b->m.s + ref->m, ref->msize, 0);
		crc = sr_crcp(r->crc, b->v.s + ref->v, ref->vsize, crc);
		crc = sr_crcp(r->crc, b->k.s + ref->k, ref->ksize, crc);
	}
	h->crcdata = crc;
	/* compression */
	if (b->compress) {
		int rc = sd_buildcompress(b, r);
		if (srunlikely(rc == -1))
			return -1;
		ref->csize = sr_bufused(&b->c) - ref->c;
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
	h->crc = sr_crcs(r->crc, h, sizeof(sdpageheader), 0);
	if (b->compress)
		memcpy(b->c.s + ref->c, h, sizeof(sdpageheader));
	return 0;
}

int sd_buildcommit(sdbuild *b, sr *r)
{
	if (b->compress_dup)
		sd_buildfree_tracker(b, r);
	if (b->compress) {
		sr_bufreset(&b->m);
		sr_bufreset(&b->v);
		sr_bufreset(&b->k);
	}
	b->n++;
	return 0;
}
