#ifndef SR_FORMAT_H_
#define SR_FORMAT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srformatref srformatref;
typedef struct srformatv   srformatv;

struct srformatref {
	uint32_t offset;
	uint16_t size;
} srpacked;

struct srformatv {
	srkeypart *part;
	srformatref r;
	char *key;
};

typedef enum {
	SR_FKV,
	SR_FDOCUMENT
} srformat;

static inline int
sr_formatsize(srformat format, srformatv *keys, int count, int vsize)
{
	switch (format) {
	case SR_FKV: {
		int sum = 0;
		int i = 0;
		while (i < count) {
			sum += keys[i].r.size;
			i++;
		}
		return sizeof(srformatref) * count + sum + vsize;
	}
	case SR_FDOCUMENT:
		return sizeof(srformatref) * count + vsize;
	}
	assert(0);
	return 0;
}

static inline void
sr_formatwrite(srformat format, char *dest, srformatv *keys, int count,
               char *v, int vsize)
{
	srformatref *ref = (srformatref*)dest;
	int offset = sizeof(srformatref) * count;
	int i = 0;
	switch (format) {
	case SR_FKV:
		while (i < count) {
			srformatv *ptr = &keys[i];
			ref->offset = offset;
			ref->size = ptr->r.size;
			memcpy(dest + offset, ptr->key, ptr->r.size);
			offset += ptr->r.size;
			ref++;
			i++;
		}
		break;
	case SR_FDOCUMENT:
		while (i < count) {
			srformatv *ptr = &keys[i];
			ref->offset = offset + (uint32_t)(ptr->key - v);
			ref->size = ptr->r.size;
			ref++;
			i++;
		}
		break;
	}
	memcpy(dest + offset, v, vsize);
}

static inline char*
sr_formatkey(char *data, int pos) {
	return data + ((srformatref*)data)[pos].offset;
}

static inline int
sr_formatkey_size(char *data, int pos) {
	return ((srformatref*)data)[pos].size;
}

static inline int
sr_formatkey_total(srkey *key, char *data)
{
	int total = 0;
	int i = 0;
	while (i < key->count) {
		total += sr_formatkey_size(data, i);
		i++;
	}
	return total + sizeof(srformatref) * key->count;
}

static inline int
sr_formatkey_copy(srkey *key, char *dest, char *src)
{
	srformatref *ref = (srformatref*)dest;
	int offset = sizeof(srformatref) * key->count;
	int i = 0;
	while (i < key->count) {
		int size = sr_formatkey_size(src, i);
		ref->offset = offset;
		ref->size = size;
		memcpy(sr_formatkey(dest, i), sr_formatkey(src, i), size);
		offset += size;
		ref++;
		i++;
	}
	return offset;
}

static inline char*
sr_formatvalue(srformat format, srkey *key, char *data)
{
	assert(key->count > 0);
	srformatref *ref = ((srformatref*)data) + (key->count - 1);
	switch (format) {
	case SR_FKV:
		return data + ref->offset + ref->size;
	case SR_FDOCUMENT:
		return data + (sizeof(srformatref) * key->count);
	}
	return NULL;
}

static inline int
sr_formatvalue_size(srformat format, srkey *key, char *data, int size) {
	assert(key->count > 0);
	switch (format) {
	case SR_FKV: {
		srformatref *ref = ((srformatref*)data) + (key->count - 1);
		return size - (ref->offset + ref->size);
	}
	case SR_FDOCUMENT:
		return size - (sizeof(srformatref) * key->count);
	}
	return 0;
}

#endif
