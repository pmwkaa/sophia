
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

static inline int
sr_astdopen(sra *a srunused, va_list args srunused) {
	return 0;
}

static inline int
sr_astdclose(sra *a srunused) {
	return 0;
}

static inline void*
sr_astdmalloc(sra *a srunused, int size) {
	return malloc(size);
}

static inline void*
sr_astdrealloc(sra *a srunused, void *ptr, int size) {
	return realloc(ptr,  size);
}

static inline void
sr_astdfree(sra *a srunused, void *ptr) {
	assert(ptr != NULL);
	free(ptr);
}

sraif sr_astd =
{
	.open    = sr_astdopen,
	.close   = sr_astdclose,
	.malloc  = sr_astdmalloc,
	.realloc = sr_astdrealloc,
	.free    = sr_astdfree 
};
