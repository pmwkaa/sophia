#ifndef SF_H_
#define SF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sfref sfref;
typedef struct sfv   sfv;

struct sfref {
	uint32_t offset;
	uint16_t size;
} sspacked;

struct sfv {
	void *part;
	char *key;
	sfref r;
};

typedef enum {
	SF_SKEYVALUE,
	SF_SRAW
} sfstorage;

typedef enum {
	SF_KV
} sf;

static inline char*
sf_key(char *data, int pos) {
	return data + ((sfref*)data)[pos].offset;
}

static inline int
sf_keysize(char *data, int pos) {
	return ((sfref*)data)[pos].size;
}

static inline int
sf_keytotal(char *data, int count)
{
	int total = 0;
	int i = 0;
	while (i < count) {
		total += sf_keysize(data, i);
		i++;
	}
	return total + sizeof(sfref) * count;
}

static inline int
sf_keycopy(char *dest, char *src, int count)
{
	sfref *ref = (sfref*)dest;
	int offset = sizeof(sfref) * count;
	int i = 0;
	while (i < count) {
		int size = sf_keysize(src, i);
		ref->offset = offset;
		ref->size = size;
		memcpy(sf_key(dest, i), sf_key(src, i), size);
		offset += size;
		ref++;
		i++;
	}
	return offset;
}

static inline char*
sf_value(sf format, char *data, int count)
{
	(void)format;

	assert(count > 0);
	sfref *ref = ((sfref*)data) + (count - 1);
	return data + ref->offset + ref->size;
}

static inline int
sf_valuesize(sf format, char *data, int size, int count)
{
	(void)format;

	assert(count > 0);
	sfref *ref = ((sfref*)data) + (count - 1);
	return size - (ref->offset + ref->size);
}

static inline int
sf_size(sf format, sfv *keys, int count, int vsize)
{
	(void)format;

	int sum = 0;
	int i = 0;
	while (i < count) {
		sum += keys[i].r.size;
		i++;
	}
	return sizeof(sfref) * count + sum + vsize;
}

static inline void
sf_write(sf format, char *dest, sfv *keys, int count,
         char *v, int vsize)
{
	(void)format;

	sfref *ref = (sfref*)dest;
	int offset = sizeof(sfref) * count;
	int i = 0;
	while (i < count) {
		sfv *ptr = &keys[i];
		ref->offset = offset;
		ref->size = ptr->r.size;
		memcpy(dest + offset, ptr->key, ptr->r.size);
		offset += ptr->r.size;
		ref++;
		i++;
	}
	memcpy(dest + offset, v, vsize);
}

static inline uint64_t
sf_hash(char *data, int count)
{
	uint64_t hash = 0;
	int i = 0;
	while (i < count) {
		hash ^= ss_fnv(sf_key(data, i), sf_keysize(data, i));
		i++;
	}
	return hash;
}

#endif
