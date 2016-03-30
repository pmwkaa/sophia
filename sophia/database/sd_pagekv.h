#ifndef SD_PAGEKV_H_
#define SD_PAGEKV_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline char*
sd_pagekv_keyread(sdpage *p, uint32_t offset, uint64_t *size)
{
	char *ptr = (char*)p->h + sizeof(sdpageheader) +
		(p->h->sizeorigin - p->h->sizekeys) + offset;
	ptr += ss_leb128read(ptr, size);
	return ptr;
}

static inline char*
sd_pagekv_key(sdpage *p, sdv *v, uint64_t *size, int part)
{
	char *ptr = sd_pagepointer(p, v);
	ptr += ss_leb128skip(ptr);
	ptr += ss_leb128skip(ptr);
	if (sv_isflags(v->flags, SVTIMESTAMP))
		ptr += ss_leb128skip(ptr);
	uint64_t offset;
	int current = 0;
	do {
		ptr += ss_leb128read(ptr, &offset);
	} while (current++ != part);

	return sd_pagekv_keyread(p, offset, size);
}

static inline char*
sd_pagekv_value(sdpage *p, sr *r, sdv *v, uint64_t *ret)
{
	uint64_t size;
	uint64_t sizekey = 0;
	char *ptr = sd_pagepointer(p, v);
	ptr += ss_leb128read(ptr, &size);
	ptr += ss_leb128skip(ptr);
	if (sv_isflags(v->flags, SVTIMESTAMP))
		ptr += ss_leb128skip(ptr);
	int i = 0;
	while (i < r->scheme->count) {
		uint64_t offset, v;
		ptr += ss_leb128read(ptr, &offset);
		sd_pagekv_keyread(p, offset, &v);
		sizekey += v;
		i++;
	}
	*ret = size - sizekey;
	return ptr;
}

static inline void
sd_pagekv_convert(sdpage *p, sr *r, sdv *v, char *dest)
{
	uint64_t size, lsn, timestamp;
	sd_pagemetaof(p, v, &size, &lsn, &timestamp);
	size += sizeof(sfref) * r->scheme->count;

	char *ptr = dest;
	memcpy(dest, v, sizeof(sdv));
	ptr += sizeof(sdv);
	ptr += ss_leb128write(ptr, size);
	ptr += ss_leb128write(ptr, lsn);
	if (sv_isflags(v->flags, SVTIMESTAMP))
		ptr += ss_leb128write(ptr, timestamp);

	assert(r->scheme->count <= 8);
	sfv kv[8];
	int i = 0;
	while (i < r->scheme->count) {
		sfv *k = &kv[i];
		k->key = sd_pagekv_key(p, v, &size, i);
		k->r.size = size;
		k->r.offset = 0;
		i++;
	}
	uint64_t valuesize;
	char *value = sd_pagekv_value(p, r, v, &valuesize);
	sf_write(SF_KV, ptr, kv, r->scheme->count,
	         value, valuesize);
}

#endif
