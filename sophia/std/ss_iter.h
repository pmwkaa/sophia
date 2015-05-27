#ifndef SS_ITER_H_
#define SS_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssiterif ssiterif;
typedef struct ssiter ssiter;

struct ssiterif {
	void  (*close)(ssiter*);
	int   (*has)(ssiter*);
	void *(*of)(ssiter*);
	void  (*next)(ssiter*);
};

struct ssiter {
	ssiterif *vif;
	char priv[120];
};

#define ss_iterinit(iterator_if, i) \
do { \
	(i)->vif = &iterator_if; \
} while (0)

#define ss_iteropen(iterator_if, i, ...) iterator_if##_open(i, __VA_ARGS__)
#define ss_iterclose(iterator_if, i) iterator_if##_close(i)
#define ss_iterhas(iterator_if, i) iterator_if##_has(i)
#define ss_iterof(iterator_if, i) iterator_if##_of(i)
#define ss_iternext(iterator_if, i) iterator_if##_next(i)

#define ss_iteratorclose(i) (i)->vif->close(i)
#define ss_iteratorhas(i) (i)->vif->has(i)
#define ss_iteratorof(i) (i)->vif->of(i)
#define ss_iteratornext(i) (i)->vif->next(i)

#endif
