#ifndef SR_A_H_
#define SR_A_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef void *(*srallocf)(void *ptr, size_t size, void *arg);

typedef struct sra sra;

struct sra {
	srallocf alloc;
	void *arg;
};

static inline void
sr_allocinit(sra *a, srallocf f, void *arg) {
	a->alloc = f;
	a->arg = arg;
}

static inline void*
sr_allocstd(void *ptr, size_t size, void *arg srunused) {
	if (srlikely(size > 0)) {
		if (ptr != NULL)
			return realloc(ptr, size);
		return malloc(size);
	}
	assert(ptr != NULL);
	free(ptr);
	return NULL;
}

static inline void*
sr_realloc(sra *a, void *ptr, size_t size) {
	return a->alloc(ptr, size, a->arg);
}

static inline void*
sr_malloc(sra *a, size_t size) {
	return a->alloc(NULL, size, a->arg);
}

static inline char*
sr_strdup(sra *a, char *str) {
	int sz = strlen(str) + 1;
	char *s = a->alloc(NULL, sz, a->arg);
	if (srunlikely(s == NULL))
		return NULL;
	memcpy(s, str, sz);
	return s;
}

static inline char*
sr_memdup(sra *a, void *ptr, size_t size) {
	char *s = a->alloc(NULL, size, a->arg);
	if (srunlikely(s == NULL))
		return NULL;
	memcpy(s, ptr, size);
	return s;
}

static inline void
sr_free(sra *a, void *ptr) {
	a->alloc(ptr, 0, a->arg);
}

#endif
