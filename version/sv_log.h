#ifndef SV_LOG_H_
#define SV_LOG_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svlog svlog;

#define SV_PREALLOC 16

struct svlog {
	sv reserve[SV_PREALLOC];
	srbuf buf;
};

static inline void
sv_loginit(svlog *l) {
	sr_bufinit_reserve(&l->buf, l->reserve, sizeof(l->reserve));
}

static inline int
sv_logsize(svlog *l) {
	return sr_bufsize(&l->buf) / sizeof(sv);
}

static inline int
sv_logn(svlog *l) {
	return sr_bufused(&l->buf) / sizeof(sv);
}

static inline void
sv_logfree(svlog *l, sra *a) {
	sr_buffree(&l->buf, a);
}

static inline int
sv_logadd(svlog *l, sra *a, sv *v) {
	return sr_bufadd(&l->buf, a, v, sizeof(sv));
}

static inline void
sv_logreplace(svlog *l, int n, sv *v) {
	sr_bufset(&l->buf, sizeof(sv), n, (char*)v, sizeof(sv));
}

#endif
