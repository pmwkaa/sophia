
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
	h->extensions  = 0;
	h->lsnmin      = UINT64_MAX;
	h->lsnmax      = 0;
	h->tsmin       = UINT32_MAX;
	h->offset      = 0;
	h->dupkeys     = 0;
	h->dupmin      = UINT64_MAX;
	memset(h->reserve, 0, sizeof(h->reserve));
	sd_idinit(&h->id, 0, 0, 0);
	i->h = NULL;
	ss_bufadvance(&i->i, sizeof(sdindexheader));
	return 0;
}

int sd_indexcommit(sdindex *i, sr *r, sdid *id, ssqf *qf, uint64_t offset)
{
	int size = ss_bufused(&i->v);
	int size_extension = 0;
	int extensions = 0;
	if (qf) {
		extensions = SD_INDEXEXT_AMQF;
		size_extension += sizeof(sdindexamqf);
		size_extension += qf->qf_table_size;
	}
	int rc = ss_bufensure(&i->i, r->a, size + size_extension);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	memcpy(i->i.p, i->v.s, size);
	ss_bufadvance(&i->i, size);
	if (qf) {
		sdindexamqf *qh = (sdindexamqf*)(i->i.p);
		qh->q       = qf->qf_qbits;
		qh->r       = qf->qf_rbits;
		qh->entries = qf->qf_entries;
		qh->size    = qf->qf_table_size;
		ss_bufadvance(&i->i, sizeof(sdindexamqf));
		memcpy(i->i.p, qf->qf_table, qf->qf_table_size);
		ss_bufadvance(&i->i, qf->qf_table_size);
	}
	ss_buffree(&i->v, r->a);
	i->h = sd_indexheader(i);
	i->h->offset     = offset;
	i->h->id         = *id;
	i->h->extension  = size_extension;
	i->h->extensions = extensions;
	i->h->crc = ss_crcs(r->crc, i->h, sizeof(sdindexheader), 0);
	return 0;
}

static inline int
sd_indexadd_raw(sdindex *i, sr *r, sdindexpage *p, char *min, char *max)
{
	/* reformat document to exclude non-key fields */
	p->sizemin = sf_comparable_size(r->scheme, min);
	p->sizemax = sf_comparable_size(r->scheme, max);
	int rc = ss_bufensure(&i->v, r->a, p->sizemin + p->sizemax);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sf_comparable_write(r->scheme, min, i->v.p);
	ss_bufadvance(&i->v, p->sizemin);
	sf_comparable_write(r->scheme, max, i->v.p);
	ss_bufadvance(&i->v, p->sizemax);
	return 0;
}

static inline int
sd_indexadd_sparse(sdindex *i, sr *r, sdbuild *build, sdindexpage *p, char *min, char *max)
{
	sfv fields[16];

	/* min */
	int part = 0;
	while (part < r->scheme->fields_count)
	{
		/* read field offset */
		uint32_t offset = *(uint32_t*)min;
		min += sizeof(uint32_t);
		/* read field */
		char *field = build->k.s + sd_buildref(build)->k + offset;
		int fieldsize = *(uint32_t*)field;
		field += sizeof(uint32_t);
		/* copy only key fields, others are set to zero */
		sfv *k = &fields[part];
		if (r->scheme->fields[part]->key) {
			k->pointer = field;
			k->size = fieldsize;
		} else {
			k->pointer = NULL;
			k->size = 0;
		}
		part++;
	}
	p->sizemin = sf_writesize(r->scheme, fields);
	int rc = ss_bufensure(&i->v, r->a, p->sizemin);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sf_write(r->scheme, fields, i->v.p);
	ss_bufadvance(&i->v, p->sizemin);

	/* max */
	part = 0;
	while (part < r->scheme->fields_count)
	{
		/* read field offset */
		uint32_t offset = *(uint32_t*)max;
		max += sizeof(uint32_t);

		/* read field */
		char *field = build->k.s + sd_buildref(build)->k + offset;
		int fieldsize = *(uint32_t*)field;
		field += sizeof(uint32_t);

		sfv *k = &fields[part];
		if (r->scheme->fields[part]->key) {
			k->pointer = field;
			k->size = fieldsize;
		} else {
			k->pointer = NULL;
			k->size = 0;
		}
		part++;
	}
	p->sizemax = sf_writesize(r->scheme, fields);
	rc = ss_bufensure(&i->v, r->a, p->sizemax);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sf_write(r->scheme, fields, i->v.p);
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
		char *min = sd_buildminkey(build);
		char *max = sd_buildmaxkey(build);
		switch (r->fmt_storage) {
		case SF_RAW:
			rc = sd_indexadd_raw(i, r, p, min, max);
			break;
		case SF_SPARSE:
			rc = sd_indexadd_sparse(i, r, build, p, min, max);
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
	if (ph->tsmin < h->tsmin)
		h->tsmin = ph->tsmin;
	h->dupkeys += ph->countdup;
	if (ph->lsnmindup < h->dupmin)
		h->dupmin = ph->lsnmindup;
	ss_bufadvance(&i->i, sizeof(sdindexpage));
	return 0;
}

int sd_indexcopy(sdindex *i, sr *r, sdindexheader *h)
{
	int size = sd_indexsize_ext(h);
	int rc = ss_bufensure(&i->i, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	memcpy(i->i.s, (char*)h, size);
	ss_bufadvance(&i->i, size);
	i->h = sd_indexheader(i);
	return 0;
}
