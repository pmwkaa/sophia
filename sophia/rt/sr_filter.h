#ifndef SR_FILTER_H_
#define SR_FILTER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srfilterif srfilterif;
typedef struct srfilter srfilter;

typedef enum {
	SR_FINPUT,
	SR_FOUTPUT
} srfilterop;

struct srfilterif {
	char *name;
	int (*init)(srfilter*, va_list);
	int (*free)(srfilter*);
	int (*reset)(srfilter*);
	int (*start)(srfilter*, srbuf*);
	int (*next)(srfilter*, srbuf*, char*, int);
	int (*complete)(srfilter*, srbuf*);
};

struct srfilter {
	srfilterif *i;
	srfilterop op;
	sr *r;
	char priv[90];
};

static inline int
sr_filterinit(srfilter *c, srfilterif *ci, sr *r, srfilterop op, ...)
{
	c->op = op;
	c->r  = r;
	c->i  = ci;
	va_list args;
	va_start(args, op);
	int rc = c->i->init(c, args);
	va_end(args);
	return rc;
}

static inline int
sr_filterfree(srfilter *c)
{
	return c->i->free(c);
}

static inline int
sr_filterreset(srfilter *c)
{
	return c->i->reset(c);
}

static inline int
sr_filterstart(srfilter *c, srbuf *dest)
{
	return c->i->start(c, dest);
}

static inline int
sr_filternext(srfilter *c, srbuf *dest, char *buf, int size)
{
	return c->i->next(c, dest, buf, size);
}

static inline int
sr_filtercomplete(srfilter *c, srbuf *dest)
{
	return c->i->complete(c, dest);
}

#endif
