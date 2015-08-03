#ifndef SV_MERGE_H_
#define SV_MERGE_H_

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
	ssiter *i, src;
	sv update;
	uint8_t dup;
	void *ptr;
} sspacked;

struct svmerge {
	svmergesrc reserve[16];
	ssbuf buf;
};

static inline void
sv_mergeinit(svmerge *m)
{
	ss_bufinit_reserve(&m->buf, m->reserve, sizeof(m->reserve));
}

static inline int
sv_mergeprepare(svmerge *m, sr *r, int count)
{
	int rc = ss_bufensure(&m->buf, r->a, sizeof(svmergesrc) * count);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	return 0;
}

static inline svmergesrc*
sv_mergenextof(svmergesrc *src)
{
	return (svmergesrc*)((char*)src + sizeof(svmergesrc));
}

static inline void
sv_mergegc(svmerge *m, ssa *a)
{
	svmergesrc *v = (svmergesrc*)m->buf.s;
	svmergesrc *end = (svmergesrc*)m->buf.p;
	while (v != end) {
		if (ssunlikely(v->update.v)) {
			sv_vfree(a, v->update.v);
			v->update.v = NULL;
		}
		v = sv_mergenextof(v);
	}
}

static inline void
sv_mergefree(svmerge *m, ssa *a)
{
	sv_mergegc(m, a);
	ss_buffree(&m->buf, a);
}

static inline void
sv_mergereset(svmerge *m, ssa *a)
{
	sv_mergegc(m, a);
	m->buf.p = m->buf.s;
}

static inline svmergesrc*
sv_mergeadd(svmerge *m, ssiter *i)
{
	assert(m->buf.p < m->buf.e);
	svmergesrc *s = (svmergesrc*)m->buf.p;
	s->dup = 0;
	s->i = i;
	memset(&s->update, 0, sizeof(s->update));
	s->ptr = NULL;
	if (i == NULL)
		s->i = &s->src;
	ss_bufadvance(&m->buf, sizeof(svmergesrc));
	return s;
}

#endif
