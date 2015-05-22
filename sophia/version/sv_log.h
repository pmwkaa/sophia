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
} srpacked;

struct svlogv {
	sv v;
	void *vgc;
	uint32_t id;
	uint32_t next;
} srpacked;

struct svlog {
	svlogindex reserve_i[4];
	svlogv reserve_v[16];
	srbuf index;
	srbuf buf;
};

static inline void
sv_loginit(svlog *l)
{
	sr_bufinit_reserve(&l->index, l->reserve_i, sizeof(l->reserve_i));
	sr_bufinit_reserve(&l->buf, l->reserve_v, sizeof(l->reserve_v));
}

static inline void
sv_logfree(svlog *l, sra *a)
{
	sr_buffree(&l->buf, a);
	sr_buffree(&l->index, a);
}

static inline int
sv_logcount(svlog *l) {
	return sr_bufused(&l->buf) / sizeof(svlogv);
}

static inline svlogv*
sv_logat(svlog *l, int pos) {
	return sr_bufat(&l->buf, sizeof(svlogv), pos);
}

static inline int
sv_logadd(svlog *l, sra *a, svlogv *v, void *ptr)
{
	uint32_t n = sv_logcount(l);
	int rc = sr_bufadd(&l->buf, a, v, sizeof(svlogv));
	if (srunlikely(rc == -1))
		return -1;
	svlogindex *i = (svlogindex*)l->index.s;
	while ((char*)i < l->index.p) {
		if (srlikely(i->id == v->id)) {
			svlogv *tail = sv_logat(l, i->tail);
			tail->next = n;
			i->tail = n;
			i->count++;
			return 0;
		}
		i++;
	}
	rc = sr_bufensure(&l->index, a, sizeof(svlogindex));
	if (srunlikely(rc == -1)) {
		l->buf.p -= sizeof(svlogv);
		return -1;
	}
	i = (svlogindex*)l->index.p;
	i->id    = v->id;
	i->head  = n;
	i->tail  = n;
	i->ptr   = ptr;
	i->count = 1;
	sr_bufadvance(&l->index, sizeof(svlogindex));
	return 0;
}

static inline void
sv_logreplace(svlog *l, int n, svlogv *v)
{
	sr_bufset(&l->buf, sizeof(svlogv), n, (char*)v, sizeof(svlogv));
}

#endif
