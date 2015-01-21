#ifndef SD_IOV_H_
#define SD_IOV_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sriov sriov;

struct sriov {
	struct iovec *v;
	int iovmax;
	int iovc;
};

static inline void
sr_iovinit(sriov *v, struct iovec *vp, int max)
{
	v->v = vp;
	v->iovc = 0;
	v->iovmax = max;
}

static inline int
sr_iovensure(sriov *v, int count) {
	return (v->iovc + count) < v->iovmax;
}

static inline int
sr_iovhas(sriov *v) {
	return v->iovc > 0;
}

static inline void
sr_iovreset(sriov *v) {
	v->iovc = 0;
}

static inline void
sr_iovadd(sriov *v, void *ptr, size_t size)
{
	assert(v->iovc < v->iovmax);
	v->v[v->iovc].iov_base = ptr;
	v->v[v->iovc].iov_len = size;
	v->iovc++;
}

#endif
