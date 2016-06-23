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
	uint32_t head, tail;
	uint32_t count;
	sr *r;
} sspacked;

struct svlogv {
	uint32_t index_id;
	uint32_t next;
	svv     *v;
	void    *ptr;
} sspacked;

struct svlog {
	int    count_write;
	svlogv reserve_buf[1];
	svlogv reserve_index[4];
	ssbuf  index;
	ssbuf  buf;
};

static inline void
sv_logvinit(svlogv *v, uint32_t id)
{
	v->index_id = id;
	v->next     = UINT32_MAX;
	v->v        = NULL;
	v->ptr      = NULL;
}

static inline int
sv_loginit(svlog *l, sr *r, int index_max)
{
	l->count_write = 0;
	ss_bufinit_reserve(&l->index, l->reserve_index, sizeof(l->reserve_index));
	ss_bufinit_reserve(&l->buf, l->reserve_buf, sizeof(l->reserve_buf));
	if (index_max == 0)
		return 0;
	int size = sizeof(svlogindex) * index_max;
	int rc = ss_bufensure(&l->index, r->a, size);
	if (ssunlikely(rc == -1))
		return -1;
	ss_bufadvance(&l->index, size);
	int i = 0;
	while (i < index_max) {
		svlogindex *index =
			ss_bufat(&l->index, sizeof(svlogindex), i);
		index->head = UINT32_MAX;
		index->tail = 0;
		index->count = 0;
		index->r = NULL;
		i++;
	}
	return 0;
}

static inline void
sv_loginit_index(svlog *l, int pos, sr *r)
{
	svlogindex *index =
		ss_bufat(&l->index, sizeof(svlogindex), pos);
	index->r = r;
}

static inline void
sv_logfree(svlog *l, sr *r)
{
	ss_buffree(&l->index, r->a);
	ss_buffree(&l->buf, r->a);
	l->count_write = 0;
}

static inline void
sv_logreset(svlog *l, int index_max)
{
	int i = 0;
	while (i < index_max) {
		svlogindex *index =
			ss_bufat(&l->index, sizeof(svlogindex), i);
		index->head  = UINT32_MAX;
		index->tail  = 0;
		index->count = 0;
		i++;
	}
	ss_bufreset(&l->buf);
	l->count_write = 0;
}

static inline int
sv_logcount(svlog *l) {
	return ss_bufused(&l->buf) / sizeof(svlogv);
}

static inline int
sv_logcount_write(svlog *l) {
	return l->count_write;
}

static inline svlogv*
sv_logat(svlog *l, int pos) {
	return ss_bufat(&l->buf, sizeof(svlogv), pos);
}

static inline svlogindex*
sv_logindex(svlog *l, int id) {
	return ss_bufat(&l->index, sizeof(svlogindex), id);
}

static inline int
sv_logadd(svlog *l, sr *r, svlogv *v)
{
	uint32_t n = sv_logcount(l);
	int rc = ss_bufadd(&l->buf, r->a, v, sizeof(svlogv));
	if (ssunlikely(rc == -1))
		return -1;
	svlogindex *index =
		ss_bufat(&l->index, sizeof(svlogindex), v->index_id);
	svlogv *tail = sv_logat(l, index->tail);
	tail->next  = n;
	if (index->head == UINT32_MAX)
		index->head = n;
	index->tail = n;
	index->count++;
	if (! (sv_vflags(v->v, r) & SVGET))
		l->count_write++;
	return 0;
}

static inline void
sv_logreplace(svlog *l, sr *r, int n, svlogv *v)
{
	svlogv *ov = sv_logat(l, n);
	if (! (sv_vflags(ov->v, r) & SVGET))
		l->count_write--;
	if (! (sv_vflags(v->v, r) & SVGET))
		l->count_write++;
	ss_bufset(&l->buf, sizeof(svlogv), n, (char*)v,
	          sizeof(svlogv));
}

#endif
