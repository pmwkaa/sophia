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
	uint8_t dup;
	sriter i;
} srpacked;

struct svmerge {
	int reserve;
	srbuf buf;
};

static inline void
sv_mergeinit(svmerge *m) {
	m->reserve = 0;
	sr_bufinit(&m->buf);
}

static inline int
sv_mergeprepare(svmerge *m, sr *r, int count, int reserve) {
	m->reserve = reserve;
	int rc = sr_bufensure(&m->buf, r->a, (sizeof(svmergesrc) + reserve) * count);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	return 0;
}

static inline void
sv_mergefree(svmerge *m, sra *a) {
	sr_buffree(&m->buf, a);
}

static inline svmergesrc*
sv_mergeadd(svmerge *m)
{
	assert(m->buf.p < m->buf.e);
	svmergesrc *s = (svmergesrc*)m->buf.p;
	s->dup = 0;
	sr_bufadvance(&m->buf, sizeof(svmergesrc) + m->reserve);
	return s;
}

extern sriterif sv_mergeiter;

#endif
