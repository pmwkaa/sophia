#ifndef SV_H_
#define SV_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SVNONE   0
#define SVDELETE 1
#define SVDUP    2
#define SVABORT  4
#define SVBEGIN  8

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
} srpacked;

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
sv_key(sv *v, sr *r srunused, int part) {
	return sr_formatkey(v->i->pointer(v), part);
}

static inline int
sv_keysize(sv *v, sr *r srunused, int part) {
	return sr_formatkey_size(v->i->pointer(v), part);
}

static inline char*
sv_value(sv *v, sr *r) {
	return sr_formatvalue(r->format, r->cmp, sv_pointer(v));
}

static inline int
sv_valuesize(sv *v, sr *r) {
	return sr_formatvalue_size(r->format, r->cmp, sv_pointer(v), sv_size(v));
}

static inline int
sv_compare(sv *a, sv *b, srkey *key) {
	return sr_compare(key, sv_pointer(a), sv_size(a),
	                       sv_pointer(b), sv_size(b));
}

#endif
