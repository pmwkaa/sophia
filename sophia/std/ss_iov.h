#ifndef SS_IOV_H_
#define SS_IOV_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssiov ssiov;

struct ssiov {
	struct iovec *v;
	int iovmax;
	int iovc;
};

static inline void
ss_iovinit(ssiov *v, struct iovec *vp, int max)
{
	v->v = vp;
	v->iovc = 0;
	v->iovmax = max;
}

static inline int
ss_iovensure(ssiov *v, int count) {
	return (v->iovc + count) < v->iovmax;
}

static inline int
ss_iovhas(ssiov *v) {
	return v->iovc > 0;
}

static inline void
ss_iovreset(ssiov *v) {
	v->iovc = 0;
}

static inline void
ss_iovadd(ssiov *v, void *ptr, size_t size)
{
	assert(v->iovc < v->iovmax);
	v->v[v->iovc].iov_base = ptr;
	v->v[v->iovc].iov_len = size;
	v->iovc++;
}

#endif
