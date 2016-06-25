#ifndef SS_A_H_
#define SS_A_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssaif ssaif;
typedef struct ssa ssa;

struct ssaif {
	int   (*open)(ssa*, va_list);
	int   (*close)(ssa*);
	void *(*malloc)(ssa*, int);
	void *(*realloc)(ssa*, void*, int);
	void  (*free)(ssa*, void*);
};

struct ssa {
	ssaif *i;
	char priv[128];
};

static inline int
ss_aopen(ssa *a, ssaif *i, ...) {
	a->i = i;
	va_list args;
	va_start(args, i);
	int rc = i->open(a, args);
	va_end(args);
	return rc;
}

static inline int
ss_aclose(ssa *a) {
	return a->i->close(a);
}

static inline void*
ss_malloc(ssa *a, int size) {
	return a->i->malloc(a, size);
}

static inline void*
ss_realloc(ssa *a, void *ptr, int size) {
	return a->i->realloc(a, ptr, size);
}

static inline void
ss_free(ssa *a, void *ptr) {
	a->i->free(a, ptr);
}

static inline char*
ss_strdup(ssa *a, char *str) {
	int sz = strlen(str) + 1;
	char *s = ss_malloc(a, sz);
	if (ssunlikely(s == NULL))
		return NULL;
	memcpy(s, str, sz);
	return s;
}

static inline char*
ss_memdup(ssa *a, void *ptr, size_t size) {
	char *s = ss_malloc(a, size);
	if (ssunlikely(s == NULL))
		return NULL;
	memcpy(s, ptr, size);
	return s;
}

#endif
