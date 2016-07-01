#ifndef SD_SEAL_H_
#define SD_SEAL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdseal sdseal;

struct sdseal {
	uint32_t  crc;
	srversion version;
	uint32_t  index_crc;
	uint64_t  index_offset;
} sspacked;

static inline void
sd_sealcreate(sdseal *s, sr *r, sdindexheader *h)
{
	sr_version_storage(&s->version);
	s->index_crc = h->crc;
	s->index_offset = h->offset;
	s->crc = ss_crcs(r->crc, s, sizeof(sdseal), 0);
}

static inline int
sd_sealvalidate(sdseal *s, sr *r, sdindexheader *h)
{
	uint32_t crc = ss_crcs(r->crc, s, sizeof(sdseal), 0);
	if (ssunlikely(s->crc != crc))
		return -1;
	if (ssunlikely(h->crc != s->index_crc))
		return -1;
	if (ssunlikely(h->offset != s->index_offset))
		return -1;
	if (ssunlikely(! sr_versionstorage_check(&s->version)))
		return -1;
	return 0;
}

#endif
