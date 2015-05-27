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
	sfref r;
	char *key;
};

typedef enum {
	SF_SKEYVALUE,
	SF_SRAW
} sfstorage;

typedef enum {
	SF_KV,
	SF_DOCUMENT
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
	assert(count > 0);
	sfref *ref = ((sfref*)data) + (count - 1);
	switch (format) {
	case SF_KV:
		return data + ref->offset + ref->size;
	case SF_DOCUMENT:
		return data + (sizeof(sfref) * count);
	}
	return NULL;
}

static inline int
sf_valuesize(sf format, char *data, int size, int count)
{
	assert(count > 0);
	switch (format) {
	case SF_KV: {
		sfref *ref = ((sfref*)data) + (count - 1);
		return size - (ref->offset + ref->size);
	}
	case SF_DOCUMENT:
		return size - (sizeof(sfref) * count);
	}
	return 0;
}

static inline int
sf_size(sf format, sfv *keys, int count, int vsize)
{
	switch (format) {
	case SF_KV: {
		int sum = 0;
		int i = 0;
		while (i < count) {
			sum += keys[i].r.size;
			i++;
		}
		return sizeof(sfref) * count + sum + vsize;
	}
	case SF_DOCUMENT:
		return sizeof(sfref) * count + vsize;
	}
	assert(0);
	return 0;
}

static inline void
sf_write(sf format, char *dest, sfv *keys, int count,
         char *v, int vsize)
{
	sfref *ref = (sfref*)dest;
	int offset = sizeof(sfref) * count;
	int i = 0;
	switch (format) {
	case SF_KV:
		while (i < count) {
			sfv *ptr = &keys[i];
			ref->offset = offset;
			ref->size = ptr->r.size;
			memcpy(dest + offset, ptr->key, ptr->r.size);
			offset += ptr->r.size;
			ref++;
			i++;
		}
		break;
	case SF_DOCUMENT:
		while (i < count) {
			sfv *ptr = &keys[i];
			ref->offset = offset + (uint32_t)(ptr->key - v);
			ref->size = ptr->r.size;
			ref++;
			i++;
		}
		break;
	}
	memcpy(dest + offset, v, vsize);
}

#endif
