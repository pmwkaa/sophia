#ifndef SV_MERGEITER_H_
#define SV_MERGEITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svmergesrc svmergesrc;
typedef struct svmerge svmerge;

struct svmergesrc {
	sriter *i, src;
	uint8_t dup;
	void *ptr;
} srpacked;

struct svmerge {
	srbuf buf;
};

static inline void
sv_mergeinit(svmerge *m) {
	sr_bufinit(&m->buf);
}

static inline int
sv_mergeprepare(svmerge *m, sr *r, int count) {
	int rc = sr_bufensure(&m->buf, r->a, sizeof(svmergesrc) * count);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	return 0;
}

static inline void
sv_mergefree(svmerge *m, sra *a) {
	sr_buffree(&m->buf, a);
}

static inline void
sv_mergereset(svmerge *m) {
	m->buf.p = m->buf.s;
}

static inline svmergesrc*
sv_mergeadd(svmerge *m, sriter *i)
{
	assert(m->buf.p < m->buf.e);
	svmergesrc *s = (svmergesrc*)m->buf.p;
	s->dup = 0;
	s->i = i;
	s->ptr = NULL;
	if (i == NULL)
		s->i = &s->src;
	sr_bufadvance(&m->buf, sizeof(svmergesrc));
	return s;
}

static inline svmergesrc*
sv_mergenextof(svmergesrc *src) {
	return (svmergesrc*)((char*)src + sizeof(svmergesrc));
}

uint32_t sv_mergeisdup(sriter*);

extern sriterif sv_mergeiter;

#endif
