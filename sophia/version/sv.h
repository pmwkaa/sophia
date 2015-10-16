#ifndef SV_H_
#define SV_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SVNONE     0
#define SVDELETE   1
#define SVUPDATE   2
#define SVGET      4
#define SVDUP      8
#define SVBEGIN    16
#define SVCONFLICT 32

typedef struct svif svif;
typedef struct sv sv;

struct svif {
	uint8_t   (*flags)(sv*);
	void      (*lsnset)(sv*, uint64_t);
	uint64_t  (*lsn)(sv*);
	char     *(*pointer)(sv*);
	uint32_t  (*size)(sv*);
};

struct sv {
	svif *i;
	void *v, *arg;
} sspacked;

static inline void
sv_init(sv *v, svif *i, void *vptr, void *arg) {
	v->i   = i;
	v->v   = vptr;
	v->arg = arg;
}

static inline uint8_t
sv_flags(sv *v) {
	return v->i->flags(v);
}

static inline int
sv_is(sv *v, uint8_t flags) {
	return (sv_flags(v) & flags) > 0;
}

static inline uint64_t
sv_lsn(sv *v) {
	return v->i->lsn(v);
}

static inline void
sv_lsnset(sv *v, uint64_t lsn) {
	v->i->lsnset(v, lsn);
}

static inline char*
sv_pointer(sv *v) {
	return v->i->pointer(v);
}

static inline uint32_t
sv_size(sv *v) {
	return v->i->size(v);
}

static inline char*
sv_key(sv *v, sr *r ssunused, int part) {
	return sf_key(v->i->pointer(v), part);
}

static inline int
sv_keysize(sv *v, sr *r ssunused, int part) {
	return sf_keysize(v->i->pointer(v), part);
}

static inline char*
sv_value(sv *v, sr *r) {
	return sf_value(r->fmt, sv_pointer(v), r->scheme->count);
}

static inline int
sv_valuesize(sv *v, sr *r) {
	return sf_valuesize(r->fmt, sv_pointer(v), sv_size(v), r->scheme->count);
}

#endif
