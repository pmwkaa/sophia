
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

int sd_indexbegin(sdindex *i, sr *r)
{
	int rc = ss_bufensure(&i->i, r->a, sizeof(sdindexheader));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdindexheader *h = sd_indexheader(i);
	sr_version(&h->version);
	h->crc         = 0;
	h->size        = 0;
	h->sizevmax    = 0;
	h->count       = 0;
	h->keys        = 0;
	h->total       = 0;
	h->totalorigin = 0;
	h->extension   = 0;
	h->lsnmin      = UINT64_MAX;
	h->lsnmax      = 0;
	h->tsmin       = 0;
	h->offset      = 0;
	h->dupkeys     = 0;
	h->dupmin      = UINT64_MAX;
	memset(h->reserve, 0, sizeof(h->reserve));
	sd_idinit(&h->id, 0, 0, 0);
	i->h = NULL;
	ss_bufadvance(&i->i, sizeof(sdindexheader));
	return 0;
}

int sd_indexcommit(sdindex *i, sr *r, sdid *id, uint64_t offset)
{
	int size = ss_bufused(&i->v);
	int rc = ss_bufensure(&i->i, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	memcpy(i->i.p, i->v.s, size);
	ss_bufadvance(&i->i, size);
	ss_buffree(&i->v, r->a);
	i->h = sd_indexheader(i);
	i->h->offset = offset;
	i->h->id     = *id;
	i->h->crc    = ss_crcs(r->crc, i->h, sizeof(sdindexheader), 0);
	return 0;
}

static inline int
sd_indexadd_raw(sdindex *i, sr *r, sdindexpage *p, char *min, char *max)
{
	/* calculate sizes */
	p->sizemin = sf_keytotal(min, r->scheme->count);
	p->sizemax = sf_keytotal(max, r->scheme->count);
	/* prepare buffer */
	int rc = ss_bufensure(&i->v, r->a, p->sizemin + p->sizemax);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	/* reformat key object to exclude value */
	rc = sf_keycopy(i->v.p, min, r->scheme->count);
	assert(rc == p->sizemin);
	(void)rc;
	ss_bufadvance(&i->v, p->sizemin);
	rc = sf_keycopy(i->v.p, max, r->scheme->count);
	assert(rc == p->sizemax);
	(void)rc;
	ss_bufadvance(&i->v, p->sizemax);
	return 0;
}

static inline int
sd_indexadd_keyvalue(sdindex *i, sr *r, sdbuild *build, sdindexpage *p, char *min, char *max)
{
	assert(r->scheme->count <= 8);

	/* min */
	sfv kv[8];
	uint64_t offset;
	int total = 0;
	int part = 0;
	while (part < r->scheme->count) {
		/* read keytab offset */
		min += ss_leb128read(min, &offset);
		/* read key */
		sfv *k = &kv[part];
		char *key = build->k.s + sd_buildref(build)->k + offset;
		uint64_t keysize;
		key += ss_leb128read(key, &keysize);
		k->key = key;
		k->r.size = keysize;
		k->r.offset = 0;
		total += keysize;
		part++;
	}
	p->sizemin = total + (r->scheme->count * sizeof(sfref));
	int rc = ss_bufensure(&i->v, r->a, p->sizemin);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sf_write(SF_KV, i->v.p, kv, r->scheme->count, NULL, 0);
	ss_bufadvance(&i->v, p->sizemin);

	/* max */
	total = 0;
	part = 0;
	while (part < r->scheme->count) {
		/* read keytab offset */
		max += ss_leb128read(max, &offset);
		/* read key */
		sfv *k = &kv[part];
		char *key = build->k.s + sd_buildref(build)->k + offset;
		uint64_t keysize;
		key += ss_leb128read(key, &keysize);
		k->key = key;
		k->r.size = keysize;
		k->r.offset = 0;
		total += keysize;
		part++;
	}
	p->sizemax = total + (r->scheme->count * sizeof(sfref));
	rc = ss_bufensure(&i->v, r->a, p->sizemax);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sf_write(SF_KV, i->v.p, kv, r->scheme->count, NULL, 0);
	ss_bufadvance(&i->v, p->sizemax);
	return 0;
}

int sd_indexadd(sdindex *i, sr *r, sdbuild *build, uint64_t offset)
{
	int rc = ss_bufensure(&i->i, r->a, sizeof(sdindexpage));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdpageheader *ph = sd_buildheader(build);

	int size = ph->size + sizeof(sdpageheader);
	int sizeorigin = ph->sizeorigin + sizeof(sdpageheader);

	/* prepare page header */
	sdindexpage *p = (sdindexpage*)i->i.p;
	p->offset      = offset;
	p->offsetindex = ss_bufused(&i->v);
	p->lsnmin      = ph->lsnmin;
	p->lsnmax      = ph->lsnmax;
	p->size        = size;
	p->sizeorigin  = sizeorigin;
	p->sizemin     = 0;
	p->sizemax     = 0;

	/* copy keys */
	if (ssunlikely(ph->count > 0))
	{
		char *min;
		char *max;
		min  = sd_buildminkey(build);
		min += ss_leb128skip(min);
		min += ss_leb128skip(min);
		max  = sd_buildmaxkey(build);
		max += ss_leb128skip(max);
		max += ss_leb128skip(max);
		switch (r->fmt_storage) {
		case SF_SRAW:
			rc = sd_indexadd_raw(i, r, p, min, max);
			break;
		case SF_SKEYVALUE:
			rc = sd_indexadd_keyvalue(i, r, build, p, min, max);
			break;
		}
		if (ssunlikely(rc == -1))
			return -1;
	}

	/* update index info */
	sdindexheader *h = sd_indexheader(i);
	h->count++;
	h->size  += sizeof(sdindexpage) + p->sizemin + p->sizemax;
	h->keys  += ph->count;
	h->total += size;
	h->totalorigin += sizeorigin;
	if (build->vmax > h->sizevmax)
		h->sizevmax = build->vmax;
	if (ph->lsnmin < h->lsnmin)
		h->lsnmin = ph->lsnmin;
	if (ph->lsnmax > h->lsnmax)
		h->lsnmax = ph->lsnmax;
	h->dupkeys += ph->countdup;
	if (ph->lsnmindup < h->dupmin)
		h->dupmin = ph->lsnmindup;
	ss_bufadvance(&i->i, sizeof(sdindexpage));
	return 0;
}

int sd_indexcopy(sdindex *i, sr *r, sdindexheader *h)
{
	int size = sd_indexsize(h);
	int rc = ss_bufensure(&i->i, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	memcpy(i->i.s, (char*)h, size);
	ss_bufadvance(&i->i, size);
	i->h = sd_indexheader(i);
	return 0;
}
