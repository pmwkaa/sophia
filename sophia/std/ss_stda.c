
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

static inline int
ss_stdaopen(ssa *a ssunused, va_list args ssunused) {
	return 0;
}

static inline int
ss_stdaclose(ssa *a ssunused) {
	return 0;
}

static inline void*
ss_stdamalloc(ssa *a ssunused, int size) {
	return malloc(size);
}

static inline void*
ss_stdarealloc(ssa *a ssunused, void *ptr, int size) {
	return realloc(ptr,  size);
}

static inline void
ss_stdafree(ssa *a ssunused, void *ptr) {
	assert(ptr != NULL);
	free(ptr);
}

ssaif ss_stda =
{
	.open    = ss_stdaopen,
	.close   = ss_stdaclose,
	.malloc  = ss_stdamalloc,
	.ensure  = NULL,
	.realloc = ss_stdarealloc,
	.free    = ss_stdafree 
};
