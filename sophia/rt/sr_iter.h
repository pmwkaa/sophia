#ifndef SR_ITER_H_
#define SR_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sriterif sriterif;
typedef struct sriter sriter;

struct sriterif {
	int   (*open)(sriter*, va_list);
	void  (*close)(sriter*);
	int   (*has)(sriter*);
	void *(*of)(sriter*);
	void  (*next)(sriter*);
};

struct sriter {
	sriterif *i;
	sr *r;
	char priv[100];
};

static inline void
sr_iterinit(sriter *i, sriterif *ii, sr *r)
{
	i->r = r;
	i->i = ii;
}

static inline int
sr_iteropen(sriter *i, ...)
{
	assert(i->i != NULL);
	va_list args;
	va_start(args, i);
	int rc = i->i->open(i, args);
	va_end(args);
	return rc;
}

static inline void
sr_iterclose(sriter *i) {
	i->i->close(i);
}

static inline int
sr_iterhas(sriter *i) {
	return i->i->has(i);
}

static inline void*
sr_iterof(sriter *i) {
	return i->i->of(i);
}

static inline void
sr_iternext(sriter *i) {
	i->i->next(i);
}

#endif
