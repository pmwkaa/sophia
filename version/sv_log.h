#ifndef SV_LOG_H_
#define SV_LOG_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svlogv svlogv;
typedef struct svlog svlog;

struct svlogv {
	uint32_t dsn;
	void *ptr;
	sv v;
} srpacked;

struct svlog {
	svlogv reserve[16];
	srbuf buf;
};

static inline void
sv_loginit(svlog *l) {
	sr_bufinit_reserve(&l->buf, l->reserve, sizeof(l->reserve));
}

static inline int
sv_logsize(svlog *l) {
	return sr_bufsize(&l->buf) / sizeof(svlogv);
}

static inline int
sv_logn(svlog *l) {
	return sr_bufused(&l->buf) / sizeof(svlogv);
}

static inline void
sv_logfree(svlog *l, sra *a) {
	sr_buffree(&l->buf, a);
}

static inline int
sv_logadd(svlog *l, sra *a, svlogv *v) {
	return sr_bufadd(&l->buf, a, v, sizeof(svlogv));
}

static inline void
sv_logreplace(svlog *l, int n, svlogv *v) {
	sr_bufset(&l->buf, sizeof(svlogv), n, (char*)v, sizeof(svlogv));
}

#endif
