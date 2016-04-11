#ifndef SD_PAGESPARSE_H_
#define SD_PAGESPARSE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline char*
sd_pagesparse_keyread(sdpage *p, uint32_t offset, uint32_t *size)
{
	char *ptr = (char*)p->h + sizeof(sdpageheader) +
	            (p->h->sizeorigin - p->h->sizekeys) + offset;
	*size = *(uint32_t*)ptr;
	return ptr + sizeof(uint32_t);
}

static inline char*
sd_pagesparse_field(sdpage *p, sdv *v, int pos, uint32_t *size)
{
	uint32_t *offsets = (uint32_t*)sd_pagepointer(p, v);
	return sd_pagesparse_keyread(p, offsets[pos], size);
}

static inline void
sd_pagesparse_convert(sdpage *p, sr *r, sdv *v, char *dest)
{
	char *ptr = dest;
	memcpy(ptr, v, sizeof(sdv));
	ptr += sizeof(sdv);
	sfv fields[8];
	int i = 0;
	while (i < r->scheme->fields_count) {
		sfv *k = &fields[i];
		k->pointer = sd_pagesparse_field(p, v, i, &k->size);
		i++;
	}
	sf_write(r->scheme, fields, ptr);
}

#endif
