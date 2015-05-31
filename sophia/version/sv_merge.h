#ifndef SV_MERGE_H_
#define SV_MERGE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svmergessc svmergessc;
typedef struct svmerge svmerge;

struct svmergessc {
	ssiter *i, ssc;
	uint8_t dup;
	void *ptr;
} sspacked;

struct svmerge {
	ssbuf buf;
};

static inline void
sv_mergeinit(svmerge *m) {
	ss_bufinit(&m->buf);
}

static inline int
sv_mergeprepare(svmerge *m, sr *r, int count) {
	int rc = ss_bufensure(&m->buf, r->a, sizeof(svmergessc) * count);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	return 0;
}

static inline void
sv_mergefree(svmerge *m, ssa *a) {
	ss_buffree(&m->buf, a);
}

static inline void
sv_mergereset(svmerge *m) {
	m->buf.p = m->buf.s;
}

static inline svmergessc*
sv_mergeadd(svmerge *m, ssiter *i)
{
	assert(m->buf.p < m->buf.e);
	svmergessc *s = (svmergessc*)m->buf.p;
	s->dup = 0;
	s->i = i;
	s->ptr = NULL;
	if (i == NULL)
		s->i = &s->ssc;
	ss_bufadvance(&m->buf, sizeof(svmergessc));
	return s;
}

static inline svmergessc*
sv_mergenextof(svmergessc *ssc) {
	return (svmergessc*)((char*)ssc + sizeof(svmergessc));
}

#endif
