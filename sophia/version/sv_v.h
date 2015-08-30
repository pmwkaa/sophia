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
	uint64_t  lsn;
	uint32_t  size;
	uint8_t   flags;
	void     *log;
	svv      *next;
	ssrbnode  node;
} sspacked;

extern svif sv_vif;

static inline char*
sv_vpointer(svv *v) {
	return (char*)(v) + sizeof(svv);
}

static inline uint32_t
sv_vsize(svv *v) {
	return sizeof(svv) + v->size;
}

static inline svv*
sv_vbuild(sr *r, sfv *keys, int count, char *data, int size)
{
	assert(r->scheme->count == count);
	int total = sf_size(r->fmt, keys, count, size);
	svv *v = ss_malloc(r->a, sizeof(svv) + total);
	if (ssunlikely(v == NULL))
		return NULL;
	v->size  = total;
	v->lsn   = 0;
	v->flags = 0;
	v->log   = NULL;
	v->next  = NULL;
	memset(&v->node, 0, sizeof(v->node));
	char *ptr = sv_vpointer(v);
	sf_write(r->fmt, ptr, keys, count, data, size);
	return v;
}

static inline svv*
sv_vbuildraw(ssa *a, char *src, int size)
{
	svv *v = ss_malloc(a, sizeof(svv) + size);
	if (ssunlikely(v == NULL))
		return NULL;
	v->size  = size;
	v->flags = 0;
	v->lsn   = 0;
	v->next  = NULL;
	v->log   = NULL;
	memset(&v->node, 0, sizeof(v->node));
	memcpy(sv_vpointer(v), src, size);
	return v;
}

static inline svv*
sv_vdup(ssa *a, sv *src)
{
	svv *v = sv_vbuildraw(a, sv_pointer(src), sv_size(src));
	if (ssunlikely(v == NULL))
		return NULL;
	v->flags = sv_flags(src);
	v->lsn   = sv_lsn(src);
	return v;
}

static inline void
sv_vfree(ssa *a, svv *v)
{
	while (v) {
		svv *n = v->next;
		ss_free(a, v);
		v = n;
	}
}

static inline svv*
sv_visible(svv *v, uint64_t vlsn) {
	while (v && v->lsn > vlsn)
		v = v->next;
	return v;
}

#endif
