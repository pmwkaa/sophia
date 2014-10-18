#ifndef SV_V_H_
#define SV_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svv svv;

struct svv {
	union {
		uint64_t lsn;
		struct {
			uint32_t id;
			uint32_t lo;
		} tx;
	} id;
	uint32_t valuesize;
	uint16_t keysize;
	uint8_t  flags;
	svv *next;
	svv *prev;
	void *log;
	srrbnode node;
} srpacked;

extern svif sv_vif;

static inline char*
sv_vkey(svv *v) {
	return (char*)(v) + sizeof(svv);
}

static inline void*
sv_vvalue(svv *v) {
	return (char*)(v) + sizeof(svv) + v->keysize;
}

static inline svv*
sv_valloc(sra *a, sv *v)
{
	int keysize = svkeysize(v);
	int valuesize = svvaluesize(v);
	int size = sizeof(svv) + keysize + valuesize;
	svv *vv = sr_malloc(a, size);
	if (srunlikely(vv == NULL))
		return NULL;
	vv->keysize   = keysize; 
	vv->valuesize = valuesize;
	vv->flags     = svflags(v);
	vv->id.lsn    = svlsn(v);
	vv->next      = NULL;
	vv->prev      = NULL;
	vv->log       = NULL;
	memset(&vv->node, 0, sizeof(vv->node));
	char *key = sv_vkey(vv);
	memcpy(key, svkey(v), keysize);
	memcpy(key + keysize, svvalue(v), valuesize);
	return vv;
}

static inline void
sv_vfree(sra *a, svv *v)
{
	while (v) {
		svv *n = v->next;
		sr_free(a, v);
		v = n;
	}
}

static inline svv*
sv_visible(svv *v, uint64_t lsvn) {
	while (v && v->id.lsn > lsvn)
		v = v->next;
	return v;
}

static inline uint32_t
sv_vsize(svv *v) {
	uint32_t size = 0;
	while (v) {
		size += sizeof(svv) + v->keysize + v->valuesize;
		v = v->next;
	}
	return size;
}

#endif
