#ifndef SS_FILTER_H_
#define SS_FILTER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssfilterif ssfilterif;
typedef struct ssfilter ssfilter;

typedef enum {
	SS_FINPUT,
	SS_FOUTPUT
} ssfilterop;

struct ssfilterif {
	char *name;
	int (*init)(ssfilter*, va_list);
	int (*free)(ssfilter*);
	int (*reset)(ssfilter*);
	int (*start)(ssfilter*, ssbuf*);
	int (*next)(ssfilter*, ssbuf*, char*, int);
	int (*complete)(ssfilter*, ssbuf*);
};

struct ssfilter {
	ssfilterif *i;
	ssfilterop op;
	ssa *a;
	char priv[90];
};

static inline int
ss_filterinit(ssfilter *c, ssfilterif *ci, ssa *a, ssfilterop op, ...)
{
	c->op = op;
	c->a  = a;
	c->i  = ci;
	va_list args;
	va_start(args, op);
	int rc = c->i->init(c, args);
	va_end(args);
	return rc;
}

static inline int
ss_filterfree(ssfilter *c)
{
	return c->i->free(c);
}

static inline int
ss_filterreset(ssfilter *c)
{
	return c->i->reset(c);
}

static inline int
ss_filterstart(ssfilter *c, ssbuf *dest)
{
	return c->i->start(c, dest);
}

static inline int
ss_filternext(ssfilter *c, ssbuf *dest, char *buf, int size)
{
	return c->i->next(c, dest, buf, size);
}

static inline int
ss_filtercomplete(ssfilter *c, ssbuf *dest)
{
	return c->i->complete(c, dest);
}

#endif
