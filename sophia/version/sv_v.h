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
	uint16_t refs;
	void    *log;
	svv     *next;
	ssrbnode node;
} sspacked;

static inline svv*
sv_vv(char *data) {
	if (ssunlikely(data == NULL))
		return NULL;
	return (svv*)(data - sizeof(svv));
}

static inline char*
sv_vpointer(svv *v) {
	return (char*)(v) + sizeof(svv);
}

static inline uint8_t
sv_vflags(svv *v, sr *r) {
	return sf_flags(r->scheme, sv_vpointer(v));
}

static inline uint32_t
sv_vsize(svv *v, sr *r) {
	return sizeof(svv) + sf_size(r->scheme, sv_vpointer(v));
}

static inline uint64_t
sv_vlsn(svv *v, sr *r) {
	return sf_lsn(r->scheme, sv_vpointer(v));
}

static inline svv*
sv_vbuild(sr *r, sfv *fields)
{
	uint32_t size = sf_writesize(r->scheme, fields);
	svv *v = ss_malloc(r->a, sizeof(svv) + size);
	if (ssunlikely(v == NULL))
		return NULL;
	v->refs  = 1;
	v->log   = NULL;
	v->next  = NULL;
	memset(&v->node, 0, sizeof(v->node));
	char *ptr = sv_vpointer(v);
	sf_write(r->scheme, fields, ptr);
	/* update runtime statistics */
	ss_spinlock(&r->stat->lock);
	r->stat->v_count++;
	r->stat->v_allocated += sizeof(svv) + size;
	ss_spinunlock(&r->stat->lock);
	return v;
}

static inline svv*
sv_vbuildraw(sr *r, char *src)
{
	uint32_t size = sf_size(r->scheme, src);
	svv *v = ss_malloc(r->a, sizeof(svv) + size);
	if (ssunlikely(v == NULL))
		return NULL;
	v->refs  = 1;
	v->log   = NULL;
	v->next  = NULL;
	memset(&v->node, 0, sizeof(v->node));
	memcpy(sv_vpointer(v), src, size);
	/* update runtime statistics */
	ss_spinlock(&r->stat->lock);
	r->stat->v_count++;
	r->stat->v_allocated += sizeof(svv) + size;
	ss_spinunlock(&r->stat->lock);
	return v;
}

static inline void
sv_vref(svv *v) {
	v->refs++;
}

static inline int
sv_vunref(sr *r, svv *v)
{
	if (sslikely(--v->refs == 0)) {
		uint32_t size = sv_vsize(v, r);
		/* update runtime statistics */
		ss_spinlock(&r->stat->lock);
		assert(r->stat->v_count > 0);
		assert(r->stat->v_allocated >= size);
		r->stat->v_count--;
		r->stat->v_allocated -= size;
		ss_spinunlock(&r->stat->lock);
		ss_free(r->a, v);
		return 1;
	}
	return 0;
}

static inline void
sv_vfree(sr *r, svv *v)
{
	while (v) {
		svv *n = v->next;
		sv_vunref(r, v);
		v = n;
	}
}

static inline svv*
sv_vvisible(svv *v, sr *r, uint64_t vlsn) {
	while (v && sv_vlsn(v, r) > vlsn)
		v = v->next;
	return v;
}

#endif
