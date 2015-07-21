#ifndef ST_OBJECT_H_
#define ST_OBJECT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

void *st_object_generate(stgenerator*, sf, stlist*, void*, uint32_t, uint32_t);
void  st_object_eq(stgenerator*, sf, void*, uint32_t, uint32_t);

static inline void*
st_object(uint32_t seed, uint32_t seed_value)
{
	return st_object_generate(&st_r.g, st_r.fmt, &st_r.gc, st_r.db,
	                          seed, seed_value);
}

static inline void
st_object_is(void *o, uint32_t seed, uint32_t seed_value)
{
	st_object_eq(&st_r.g, st_r.fmt, o, seed, seed_value);
}

#endif
