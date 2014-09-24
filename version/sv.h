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
	uint64_t  (*lsn)(sv*);
	void      (*lsnset)(sv*, uint64_t);
	char     *(*key)(sv*);
	uint16_t  (*keysize)(sv*);
	char     *(*value)(sv*);
	int       (*valuecopy)(sv*, char*);
	uint32_t  (*valuesize)(sv*);
	uint64_t  (*valueoffset)(sv*);
	char     *(*raw)(sv*);
	uint32_t  (*rawsize)(sv*);
	void      (*ref)(sv*);
	void      (*unref)(sv*, sra*);
};

struct sv {
	svif *i;
	void *v, *arg;
	uint8_t flags;
} srpacked;

static inline void
svinit(sv *v, svif *i, void *vptr, void *arg) {
	v->i     = i;
	v->v     = vptr;
	v->arg   = arg;
	v->flags = 0;
}

static inline uint8_t
svflags(sv *v) {
	return v->i->flags(v) | v->flags;
}

static inline uint64_t
svlsn(sv *v) {
	return v->i->lsn(v);
}

static inline void
svlsnset(sv *v, uint64_t lsn) {
	v->i->lsnset(v, lsn);
}

static inline char*
svkey(sv *v) {
	return v->i->key(v);
}

static inline uint16_t
svkeysize(sv *v) {
	return v->i->keysize(v);
}

static inline char*
svvalue(sv *v) {
	return v->i->value(v);
}

static inline int
svvaluecopy(sv *v, char *dest) {
	return v->i->valuecopy(v, dest);
}

static inline uint32_t
svvaluesize(sv *v) {
	return v->i->valuesize(v);
}

static inline uint64_t
svvalueoffset(sv *v) {
	return v->i->valueoffset(v);
}

static inline char*
svraw(sv *v) {
	return v->i->raw(v);
}

static inline uint32_t
svrawsize(sv *v) {
	return v->i->rawsize(v);
}

static inline void
svref(sv *v) {
	v->i->ref(v);
}

static inline void
svunref(sv *v, sra *a) {
	v->i->unref(v, a);
}

static inline void
svsetdup(sv *v) {
	v->flags = SVDUP;
}

static inline int
svcompare(sv *a, sv *b, srcomparator *c) {
	return sr_compare(c, svkey(a), svkeysize(a),
	                     svkey(b), svkeysize(b));
}

#endif
