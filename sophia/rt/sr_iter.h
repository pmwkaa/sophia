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
	void  (*close)(sriter*);
	int   (*has)(sriter*);
	void *(*of)(sriter*);
	void  (*next)(sriter*);
};

struct sriter {
	sriterif *vif;
	sr *r;
	char priv[100];
};

#define sr_iterinit(iterator_if, i, r_) \
do { \
	(i)->r = r_; \
	(i)->vif = &iterator_if; \
} while (0)

#define sr_iteropen(iterator_if, i, ...) iterator_if##_open(i, __VA_ARGS__)
#define sr_iterclose(iterator_if, i) iterator_if##_close(i)
#define sr_iterhas(iterator_if, i) iterator_if##_has(i)
#define sr_iterof(iterator_if, i) iterator_if##_of(i)
#define sr_iternext(iterator_if, i) iterator_if##_next(i)

#define sr_iteratorclose(i) (i)->vif->close(i)
#define sr_iteratorhas(i) (i)->vif->has(i)
#define sr_iteratorof(i) (i)->vif->of(i)
#define sr_iteratornext(i) (i)->vif->next(i)

#endif
