#ifndef ST_DOCUMENT_H_
#define ST_DOCUMENT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

void *st_document_generate(stgenerator*, stlist*, void*, uint32_t, uint32_t);
void  st_document_eq(stgenerator*, void*, uint32_t, uint32_t);

static inline void*
st_document(uint32_t seed, uint32_t seed_value)
{
	return st_document_generate(&st_r.g, &st_r.gc, st_r.db,
	                            seed, seed_value);
}

static inline void
st_document_is(void *o, uint32_t seed, uint32_t seed_value)
{
	st_document_eq(&st_r.g, o, seed, seed_value);
}

#endif
