
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

static inline int
sr_stdaopen(sra *a srunused, va_list args srunused) {
	return 0;
}

static inline int
sr_stdaclose(sra *a srunused) {
	return 0;
}

static inline void*
sr_stdamalloc(sra *a srunused, int size) {
	return malloc(size);
}

static inline void*
sr_stdarealloc(sra *a srunused, void *ptr, int size) {
	return realloc(ptr,  size);
}

static inline void
sr_stdafree(sra *a srunused, void *ptr) {
	assert(ptr != NULL);
	free(ptr);
}

sraif sr_stda =
{
	.open    = sr_stdaopen,
	.close   = sr_stdaclose,
	.malloc  = sr_stdamalloc,
	.realloc = sr_stdarealloc,
	.free    = sr_stdafree 
};
