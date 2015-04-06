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
#define SVSET    1
#define SVDELETE 2
#define SVDUP    4
#define SVABORT  8
#define SVBEGIN  16

typedef struct svif svif;
typedef struct sv sv;

struct svif {
	uint8_t   (*flags)(sv*);
	void      (*lsnset)(sv*, uint64_t);
	uint64_t  (*lsn)(sv*);
	char     *(*key)(sv*);
	uint16_t  (*keysize)(sv*);
	char     *(*value)(sv*);
	uint32_t  (*valuesize)(sv*);
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
sv_key(sv *v) {
	return v->i->key(v);
}

static inline uint16_t
sv_keysize(sv *v) {
	return v->i->keysize(v);
}

static inline char*
sv_value(sv *v) {
	return v->i->value(v);
}

static inline uint32_t
sv_valuesize(sv *v) {
	return v->i->valuesize(v);
}

static inline int
sv_compare(sv *a, sv *b, srcomparator *c) {
	return sr_compare(c, sv_key(a), sv_keysize(a),
	                     sv_key(b), sv_keysize(b));
}

#endif
