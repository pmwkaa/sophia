#ifndef SV_LOG_H_
#define SV_LOG_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svlogindex svlogindex;
typedef struct svlogv svlogv;
typedef struct svlog svlog;

struct svlogindex {
	uint32_t id;
	uint32_t head, tail;
	uint32_t count;
	void *ptr;
} sspacked;

struct svlogv {
	sv v;
	uint32_t id;
	uint32_t next;
} sspacked;

struct svlog {
	svlogindex reserve_i[4];
	svlogv reserve_v[16];
	ssbuf index;
	ssbuf buf;
};

static inline void
sv_logvinit(svlogv *v, uint32_t id)
{
	v->id   = id;
	v->next = UINT32_MAX;
	v->v.v  = NULL;
	v->v.i  = NULL;
}

static inline void
sv_loginit(svlog *l)
{
	ss_bufinit_reserve(&l->index, l->reserve_i, sizeof(l->reserve_i));
	ss_bufinit_reserve(&l->buf, l->reserve_v, sizeof(l->reserve_v));
}

static inline void
sv_logfree(svlog *l, ssa *a)
{
	ss_buffree(&l->buf, a);
	ss_buffree(&l->index, a);
}

static inline int
sv_logcount(svlog *l) {
	return ss_bufused(&l->buf) / sizeof(svlogv);
}

static inline svlogv*
sv_logat(svlog *l, int pos) {
	return ss_bufat(&l->buf, sizeof(svlogv), pos);
}

static inline int
sv_logadd(svlog *l, ssa *a, svlogv *v, void *ptr)
{
	uint32_t n = sv_logcount(l);
	int rc = ss_bufadd(&l->buf, a, v, sizeof(svlogv));
	if (ssunlikely(rc == -1))
		return -1;
	svlogindex *i = (svlogindex*)l->index.s;
	while ((char*)i < l->index.p) {
		if (sslikely(i->id == v->id)) {
			svlogv *tail = sv_logat(l, i->tail);
			tail->next = n;
			i->tail = n;
			i->count++;
			return 0;
		}
		i++;
	}
	rc = ss_bufensure(&l->index, a, sizeof(svlogindex));
	if (ssunlikely(rc == -1)) {
		l->buf.p -= sizeof(svlogv);
		return -1;
	}
	i = (svlogindex*)l->index.p;
	i->id    = v->id;
	i->head  = n;
	i->tail  = n;
	i->ptr   = ptr;
	i->count = 1;
	ss_bufadvance(&l->index, sizeof(svlogindex));
	return 0;
}

static inline void
sv_logreplace(svlog *l, int n, svlogv *v)
{
	ss_bufset(&l->buf, sizeof(svlogv), n, (char*)v, sizeof(svlogv));
}

#endif
