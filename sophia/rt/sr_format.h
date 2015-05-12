#ifndef SR_FORMAT_H_
#define SR_FORMAT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srfmtref srfmtref;
typedef struct srfmtv   srfmtv;

struct srfmtref {
	uint32_t offset;
	uint16_t size;
} srpacked;

struct srfmtv {
	srkeypart *part;
	srfmtref r;
	char *key;
};

typedef enum {
	SR_FS_KEYVALUE,
	SR_FS_RAW
} srfmtstorage;

typedef enum {
	SR_FKV,
	SR_FDOCUMENT
} srfmt;

static inline int
sr_fmtsize(srfmt format, srfmtv *keys, int count, int vsize)
{
	switch (format) {
	case SR_FKV: {
		int sum = 0;
		int i = 0;
		while (i < count) {
			sum += keys[i].r.size;
			i++;
		}
		return sizeof(srfmtref) * count + sum + vsize;
	}
	case SR_FDOCUMENT:
		return sizeof(srfmtref) * count + vsize;
	}
	assert(0);
	return 0;
}

static inline void
sr_fmtwrite(srfmt format, char *dest, srfmtv *keys, int count,
               char *v, int vsize)
{
	srfmtref *ref = (srfmtref*)dest;
	int offset = sizeof(srfmtref) * count;
	int i = 0;
	switch (format) {
	case SR_FKV:
		while (i < count) {
			srfmtv *ptr = &keys[i];
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
			srfmtv *ptr = &keys[i];
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
sr_fmtkey(char *data, int pos) {
	return data + ((srfmtref*)data)[pos].offset;
}

static inline int
sr_fmtkey_size(char *data, int pos) {
	return ((srfmtref*)data)[pos].size;
}

static inline int
sr_fmtkey_total(srkey *key, char *data)
{
	int total = 0;
	int i = 0;
	while (i < key->count) {
		total += sr_fmtkey_size(data, i);
		i++;
	}
	return total + sizeof(srfmtref) * key->count;
}

static inline int
sr_fmtkey_copy(srkey *key, char *dest, char *src)
{
	srfmtref *ref = (srfmtref*)dest;
	int offset = sizeof(srfmtref) * key->count;
	int i = 0;
	while (i < key->count) {
		int size = sr_fmtkey_size(src, i);
		ref->offset = offset;
		ref->size = size;
		memcpy(sr_fmtkey(dest, i), sr_fmtkey(src, i), size);
		offset += size;
		ref++;
		i++;
	}
	return offset;
}

static inline char*
sr_fmtvalue(srfmt format, srkey *key, char *data)
{
	assert(key->count > 0);
	srfmtref *ref = ((srfmtref*)data) + (key->count - 1);
	switch (format) {
	case SR_FKV:
		return data + ref->offset + ref->size;
	case SR_FDOCUMENT:
		return data + (sizeof(srfmtref) * key->count);
	}
	return NULL;
}

static inline int
sr_fmtvalue_size(srfmt format, srkey *key, char *data, int size)
{
	assert(key->count > 0);
	switch (format) {
	case SR_FKV: {
		srfmtref *ref = ((srfmtref*)data) + (key->count - 1);
		return size - (ref->offset + ref->size);
	}
	case SR_FDOCUMENT:
		return size - (sizeof(srfmtref) * key->count);
	}
	return 0;
}

#endif
