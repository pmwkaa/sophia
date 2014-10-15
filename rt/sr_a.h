#ifndef SR_A_H_
#define SR_A_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sraif sraif;
typedef struct sra sra;

struct sraif {
	int   (*open)(sra*, va_list);
	int   (*close)(sra*);
	void *(*malloc)(sra*, int);
	void *(*realloc)(sra*, void*, int);
	void  (*free)(sra*, void*);
};

struct sra {
	sraif *i;
	char priv[48];
};

static inline int
sr_allocopen(sra *a, sraif *i, ...) {
	a->i = i;
	va_list args;
	va_start(args, i);
	int rc = i->open(a, args);
	va_end(args);
	return rc;
}

static inline int
sr_allocclose(sra *a) {
	return a->i->close(a);
}

static inline void*
sr_malloc(sra *a, int size) {
	return a->i->malloc(a, size);
}

static inline void*
sr_realloc(sra *a, void *ptr, int size) {
	return a->i->realloc(a, ptr, size);
}

static inline void
sr_free(sra *a, void *ptr) {
	a->i->free(a, ptr);
}

static inline char*
sr_strdup(sra *a, char *str) {
	int sz = strlen(str) + 1;
	char *s = sr_malloc(a, sz);
	if (srunlikely(s == NULL))
		return NULL;
	memcpy(s, str, sz);
	return s;
}

static inline char*
sr_memdup(sra *a, void *ptr, size_t size) {
	char *s = sr_malloc(a, size);
	if (srunlikely(s == NULL))
		return NULL;
	memcpy(s, ptr, size);
	return s;
}

#endif
