#ifndef SF_H_
#define SF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sfvar sfvar;
typedef struct sfv sfv;

#define SVNONE   0
#define SVDELETE 1
#define SVUPSERT 2
#define SVGET    4
#define SVDUP    8
#define SVBEGIN  16

struct sfvar {
	uint32_t offset;
	uint32_t size;
} sspacked;

struct sfv {
	char     *pointer;
	uint32_t  size;
};

static inline uint32_t
sf_ttl(sfscheme *s, char *data)
{
	assert(s->has_expire);
	return *(uint32_t*)(data + s->offset_expire);
}

static inline uint8_t
sf_flags(sfscheme *s, char *data)
{
	assert(s->has_flags);
	return *(uint8_t*)(data + s->offset_flags);
}

static inline int
sf_flagsequ(uint8_t flags, uint8_t value) {
	return (flags & value) > 0;
}

static inline int
sf_is(sfscheme *s, char *data, uint8_t flags) {
	return sf_flagsequ(sf_flags(s, data), flags) > 0;
}

static inline void
sf_flagsset(sfscheme *s, char *data, uint8_t flags)
{
	assert(s->has_flags);
	*(uint8_t*)(data + s->offset_flags) = flags;
}

static inline uint32_t
sf_size(sfscheme *s, char *data)
{
	assert(s->has_size);
	return *(uint32_t*)(data + s->offset_size);
}

static inline void
sf_sizeset(sfscheme *s, char *data, uint32_t size)
{
	assert(s->has_size);
	*(uint32_t*)(data + s->offset_size) = size;
}

static inline uint64_t
sf_lsn(sfscheme *s, char *data)
{
	assert(s->has_lsn);
	return *(uint64_t*)(data + s->offset_lsn);
}

static inline void
sf_lsnset(sfscheme *s, char *data, uint64_t lsn)
{
	assert(s->has_lsn);
	*(uint64_t*)(data + s->offset_lsn) = lsn;
}

static inline char*
sf_fieldof_ptr(sfscheme *s, sffield *f, char *data, uint32_t *size)
{
	if (sslikely(f->fixed_size > 0)) {
		if (sslikely(size))
			*size = f->fixed_size;
		return data + f->fixed_offset;
	}
	register sfvar *v =
		&((sfvar*)(data + s->var_offset))[f->position_ref];
	if (sslikely(size))
		*size = v->size;
	return data + v->offset;
}

static inline char*
sf_fieldof(sfscheme *s, int pos, char *data, uint32_t *size)
{
	return sf_fieldof_ptr(s, s->fields[pos], data, size);
}

static inline char*
sf_field(sfscheme *s, int pos, char *data)
{
	register sffield *f = s->fields[pos];
	if (sslikely(f->fixed_size > 0))
		return data + f->fixed_offset;
	register sfvar *v =
		&((sfvar*)(data + s->var_offset))[f->position_ref];
	return data + v->offset;
}

static inline int
sf_fieldsize(sfscheme *s, int pos, char *data)
{
	register sffield *f = s->fields[pos];
	if (sslikely(f->fixed_size > 0))
		return f->fixed_size;
	register sfvar *v =
		&((sfvar*)(data + s->var_offset))[f->position_ref];
	return v->size;
}

static inline int
sf_writesize(sfscheme *s, sfv *v)
{
	int sum = s->var_offset;
	int i;
	for (i = 0; i < s->fields_count; i++) {
		sffield *f = s->fields[i];
		if (f->fixed_size != 0)
			continue;
		sum += sizeof(sfvar)+ v[i].size;
	}
	return sum;
}

static inline void
sf_write(sfscheme *s, sfv *v, char *dest)
{
	int var_value_offset =
		s->var_offset + sizeof(sfvar) * s->var_count;
	sfvar *var = (sfvar*)(dest + s->var_offset);
	int i;
	for (i = 0; i < s->fields_count; i++) {
		sffield *f = s->fields[i];
		if (f->fixed_size) {
			if (sslikely(v[i].size > 0))
				memcpy(dest + f->fixed_offset, v[i].pointer, v[i].size);
			else
				memset(dest + f->fixed_offset, 0, f->fixed_size);
			continue;
		}
		sfvar *current = &var[f->position_ref];
		current->offset = var_value_offset;
		current->size   = v[i].size;
		if (sslikely(v[i].size > 0))
			memcpy(dest + var_value_offset, v[i].pointer, v[i].size);
		var_value_offset += current->size;
	}
}

static inline uint64_t
sf_hash(sfscheme *s, char *data)
{
	uint64_t hash = 0;
	int i;
	for (i = 0; i < s->keys_count; i++)
		hash ^= ss_fnv(sf_field(s, i, data), sf_fieldsize(s, i, data));
	return hash;
}

static inline int
sf_comparable_size(sfscheme *s, char *data)
{
	int sum = s->var_offset;
	int i;
	for (i = 0; i < s->fields_count; i++) {
		sffield *f = s->fields[i];
		if (f->fixed_size != 0)
			continue;
		if (f->key)
			sum += sf_fieldsize(s, i, data);
		sum += sizeof(sfvar);
	}
	return sum;
}

static inline void
sf_comparable_write(sfscheme *s, char *src, char *dest)
{
	int var_value_offset =
		s->var_offset + sizeof(sfvar) * s->var_count;
	memcpy(dest, src, s->var_offset);
	sfvar *var = (sfvar*)(dest + s->var_offset);
	int i;
	for (i = 0; i < s->fields_count; i++) {
		sffield *f = s->fields[i];
		if (f->fixed_size != 0)
			continue;
		sfvar *current = &var[f->position_ref];
		current->offset = var_value_offset;
		if (! f->key) {
			current->size = 0;
			continue;
		}
		char *ptr = sf_fieldof_ptr(s, f, src, &current->size);
		memcpy(dest + var_value_offset, ptr, current->size);
		var_value_offset += current->size;
	}
}

#endif
